/*
 * Copyright (C) 2026 John Garner
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.pikatimer.util;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javafx.animation.PauseTransition;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyBooleanProperty;
import javafx.beans.property.ReadOnlyBooleanWrapper;
import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;
import javafx.collections.ObservableListBase;
import javafx.util.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A filtered and sorted view of an ObservableList that does the filtering and
 * sorting on a background thread.
 *
 * FilteredList / SortedList do all of their work on the JavaFX thread and a
 * SortedList walks its whole index for every change. That is fine for a few
 * thousand rows but locks up the UI once there are hundreds of thousands.
 *
 * This list instead:
 *  - Takes a snapshot of the source on the JavaFX thread (an array copy)
 *  - Filters and sorts the snapshot on a background thread. The sort keys are
 *    pulled out of each row once (decorate-sort-undecorate) so an expensive
 *    key is computed n times and not n log n times.
 *  - When only new rows show up it filters/sorts just those and merges them
 *    into the current result.
 *  - Publishes the result back on the JavaFX thread as one single change, at
 *    most once per throttle period.
 *
 * The filter predicate and the sort keys run on the background thread, so
 * they must only use data that is safe to read from there (immutable
 * snapshots, plain fields).
 *
 * Must be created and used on the JavaFX application thread.
 */
public class BackgroundSortedFilteredList<T> extends ObservableListBase<T> {
    private static final Logger logger = LoggerFactory.getLogger(BackgroundSortedFilteredList.class);

    /** Use parallel streams / sorts above this many rows */
    private static final int PARALLEL_THRESHOLD = 50_000;

    /** More inserts than this in one merge are published as one replace */
    private static final int MAX_INCREMENTAL_INSERTS = 2_000;

    private static final Row[] NO_ROWS = new Row[0];

    private final ObservableList<? extends T> source;
    private final ExecutorService worker;
    private final long throttleMillis;
    private final PauseTransition timer = new PauseTransition();

    // Everything below is only touched on the JavaFX thread
    private Row[] rows = NO_ROWS;           // the published content
    private Predicate<? super T> filter = null;
    private List<SortKey<? super T>> sortKeys = List.of();
    private int specGeneration = 0;         // bumped for every filter / sort change
    private boolean specChangedSincePublish = false;
    private boolean fullRebuildNeeded = true;
    private final List<T> pendingAdds = new ArrayList<>();
    private boolean jobRunning = false;
    private boolean urgentPending = false;
    private long lastStartNanos = 0;
    private final ReadOnlyBooleanWrapper busy = new ReadOnlyBooleanWrapper(false);

    private Consumer<Update> beforeUpdate;
    private Consumer<Update> afterUpdate;

    /** What kind of change is being published (see setUpdateHooks) */
    public enum Update {
        /** New source rows were inserted into the current view */
        ROWS_ADDED,
        /** The whole view was replaced because of changes to the source */
        SOURCE_REPLACED,
        /** The whole view was replaced because the filter or sort changed */
        VIEW_CHANGED
    }

    /**
     * @param source the list to view. Only modified on the JavaFX thread.
     * @param threadName name for the background worker thread
     * @param throttle the minimum time between publishing updates caused by
     * changes to the source list. Filter and sort changes are not throttled.
     */
    public BackgroundSortedFilteredList(ObservableList<? extends T> source, String threadName, Duration throttle) {
        this.source = source;
        this.throttleMillis = (long) throttle.toMillis();
        this.worker = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, threadName);
            t.setDaemon(true);
            t.setPriority(Thread.NORM_PRIORITY - 1);
            return t;
        });
        timer.setOnFinished(e -> startJob());

        source.addListener((ListChangeListener.Change<? extends T> c) -> {
            while (c.next()) {
                if (c.wasPermutated() || c.wasRemoved()) {
                    // removes, replaces, and re-orders: start over
                    fullRebuildNeeded = true;
                } else if (c.wasAdded()) {
                    if (!fullRebuildNeeded) pendingAdds.addAll(c.getAddedSubList());
                }
                // Updates need nothing, the rows point at the same objects. Use
                // refresh() if a change should move a row or filter it out.
            }
            if (fullRebuildNeeded) pendingAdds.clear();
            requestUpdate(false);
        });

        // Give the owner a chance to set the filter and sort before the first pass
        Platform.runLater(() -> requestUpdate(true));
    }

    /**
     * One part of the sort order: a key pulled out of each row and the
     * comparator for that key. Null keys always sort last.
     */
    public static final class SortKey<T> {
        private final Function<? super T, ?> extractor;
        private final Comparator<Object> comparator;

        @SuppressWarnings("unchecked")
        private SortKey(Function<? super T, ?> extractor, Comparator<?> comparator) {
            this.extractor = extractor;
            this.comparator = (Comparator<Object>) comparator;
        }

        public static <T, K> SortKey<T> of(Function<? super T, ? extends K> extractor, Comparator<? super K> comparator, boolean descending) {
            Comparator<? super K> c = descending ? comparator.reversed() : comparator;
            return new SortKey<>(extractor, Comparator.nullsLast(c));
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        public static <T, K extends Comparable<? super K>> SortKey<T> natural(Function<? super T, ? extends K> extractor, boolean descending) {
            return of(extractor, (Comparator) Comparator.naturalOrder(), descending);
        }
    }

    // A row and its sort keys
    private static final class Row {
        final Object item;
        final Object[] keys;
        Row(Object item, Object[] keys) {
            this.item = item;
            this.keys = keys;
        }
    }

    // The result of a background pass. positions is null for a full rebuild,
    // otherwise it holds the (ascending) index of every inserted row.
    private record Result(Row[] rows, int[] positions) {}

    /** Only show rows that match. null shows everything. */
    public void setFilter(Predicate<? super T> filter) {
        this.filter = filter;
        specChanged();
    }

    /** The sort order, first key first. An empty list keeps the source order. */
    public void setSortKeys(List<? extends SortKey<? super T>> keys) {
        this.sortKeys = List.copyOf(keys);
        specChanged();
    }

    /** Filter and sort everything again, e.g. after a row changed in a way that matters. */
    public void refresh() {
        fullRebuildNeeded = true;
        pendingAdds.clear();
        requestUpdate(true);
    }

    /**
     * Hooks run on the JavaFX thread just before and just after every change
     * is published.
     *
     * A TableView does not keep its selection right through these changes: it
     * does not shift the selected index for a change made of many separate
     * inserts, and it is slow to work out the selection after a replace of a
     * very large list. The hooks let the owner save the selection (and clear
     * it before a replace) and put it back afterwards.
     */
    public void setUpdateHooks(Consumer<Update> before, Consumer<Update> after) {
        this.beforeUpdate = before;
        this.afterUpdate = after;
    }

    /** True while a background pass is running */
    public ReadOnlyBooleanProperty busyProperty() {
        return busy.getReadOnlyProperty();
    }

    /** indexOf by identity, without going through equals() */
    public int indexOfIdentity(Object o) {
        if (o == null) return -1;
        final Row[] r = rows;
        for (int i = 0; i < r.length; i++) if (r[i].item == o) return i;
        return -1;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T get(int index) {
        return (T) rows[index].item;
    }

    @Override
    public int size() {
        return rows.length;
    }

    private void specChanged() {
        specGeneration++;
        specChangedSincePublish = true;
        fullRebuildNeeded = true;
        pendingAdds.clear();
        requestUpdate(true);
    }

    private void requestUpdate(boolean urgent) {
        if (jobRunning) {
            // picked up when the running job is done
            urgentPending |= urgent;
            return;
        }
        if (!fullRebuildNeeded && pendingAdds.isEmpty()) return;

        long sinceLast = (System.nanoTime() - lastStartNanos) / 1_000_000;
        long delay = urgent ? 0 : Math.max(0, throttleMillis - sinceLast);
        if (delay == 0) {
            timer.stop();
            startJob();
        } else if (timer.getStatus() != javafx.animation.Animation.Status.RUNNING) {
            timer.setDuration(Duration.millis(delay));
            timer.playFromStart();
        }
    }

    private void startJob() {
        if (jobRunning || (!fullRebuildNeeded && pendingAdds.isEmpty())) return;

        // A big batch of adds (e.g. the initial load) would be published as a
        // full replace anyway, and a straight rebuild is cheaper than a merge.
        final boolean full = fullRebuildNeeded || pendingAdds.size() > MAX_INCREMENTAL_INSERTS;
        final Object[] input = full ? source.toArray() : pendingAdds.toArray();
        pendingAdds.clear();
        fullRebuildNeeded = false;

        final Row[] base = rows;
        final Predicate<? super T> f = filter;
        final List<SortKey<? super T>> keys = sortKeys;
        final int generation = specGeneration;

        jobRunning = true;
        busy.set(true);
        lastStartNanos = System.nanoTime();

        worker.execute(() -> {
            Result result = null;
            try {
                long start = System.nanoTime();
                result = full ? rebuild(input, f, keys) : merge(base, input, f, keys);
                logger.trace("{} pass over {} rows took {}ms", full ? "Full" : "Incremental", input.length, (System.nanoTime() - start) / 1_000_000);
            } catch (Throwable t) {
                logger.error("Unable to filter/sort the list", t);
            }
            final Result r = result;
            Platform.runLater(() -> finishJob(r, generation));
        });
    }

    private void finishJob(Result result, int generation) {
        jobRunning = false;

        // A newer filter or sort is waiting, so this result is already out of date
        if (result != null && generation == specGeneration) publish(result);

        boolean urgent = urgentPending;
        urgentPending = false;
        busy.set(fullRebuildNeeded || !pendingAdds.isEmpty());
        requestUpdate(urgent);
    }

    private void publish(Result result) {
        final Row[] old = rows;

        if (result.positions() != null && result.positions().length <= MAX_INCREMENTAL_INSERTS) {
            if (result.positions().length == 0) return;
            if (beforeUpdate != null) beforeUpdate.accept(Update.ROWS_ADDED);
            rows = result.rows();
            beginChange();
            for (int p: result.positions()) nextAdd(p, p + 1);
            endChange();
            if (afterUpdate != null) afterUpdate.accept(Update.ROWS_ADDED);
            return;
        }

        final Update kind = specChangedSincePublish ? Update.VIEW_CHANGED : Update.SOURCE_REPLACED;
        specChangedSincePublish = false;
        if (old.length == 0 && result.rows().length == 0) return;

        if (beforeUpdate != null) beforeUpdate.accept(kind);
        rows = result.rows();
        beginChange();
        if (old.length > 0) nextRemove(0, new RowItems<T>(old));
        if (rows.length > 0) nextAdd(0, rows.length);
        endChange();
        if (afterUpdate != null) afterUpdate.accept(kind);
    }

    // A read only List view of the items in a Row[] (for the removed list in a change)
    private static final class RowItems<T> extends AbstractList<T> {
        private final Row[] r;
        RowItems(Row[] r) { this.r = r; }
        @Override @SuppressWarnings("unchecked") public T get(int i) { return (T) r[i].item; }
        @Override public int size() { return r.length; }
    }

    // Background thread from here down

    @SuppressWarnings("unchecked")
    private Row[] decorate(Object[] input, Predicate<? super T> f, List<SortKey<? super T>> keys) {
        Stream<Object> s = Arrays.stream(input);
        if (input.length > PARALLEL_THRESHOLD) s = s.parallel();
        if (f != null) s = s.filter(o -> f.test((T) o));
        final int k = keys.size();
        Row[] out = s.map(o -> {
                Object[] values = new Object[k];
                for (int i = 0; i < k; i++) values[i] = keys.get(i).extractor.apply((T) o);
                return new Row(o, values);
            }).toArray(Row[]::new);

        // both sorts are stable, so equal rows stay in source order
        if (k > 0) {
            Comparator<Row> c = rowComparator(keys);
            if (out.length > PARALLEL_THRESHOLD) Arrays.parallelSort(out, c);
            else Arrays.sort(out, c);
        }
        return out;
    }

    private Result rebuild(Object[] input, Predicate<? super T> f, List<SortKey<? super T>> keys) {
        return new Result(decorate(input, f, keys), null);
    }

    private Result merge(Row[] base, Object[] input, Predicate<? super T> f, List<SortKey<? super T>> keys) {
        Row[] add = decorate(input, f, keys);
        if (add.length == 0) return new Result(base, new int[0]);

        Comparator<Row> c = rowComparator(keys);
        Row[] out = new Row[base.length + add.length];
        int[] positions = new int[add.length];
        int from = 0; // next row of base to copy
        int to = 0;   // next free slot in out
        for (int j = 0; j < add.length; j++) {
            // New rows go after any existing rows that are equal to them, same
            // as appending to the source and doing a stable sort.
            int at = keys.isEmpty() ? base.length : upperBound(base, from, add[j], c);
            int n = at - from;
            System.arraycopy(base, from, out, to, n);
            to += n;
            from = at;
            positions[j] = to;
            out[to++] = add[j];
        }
        System.arraycopy(base, from, out, to, base.length - from);
        return new Result(out, positions);
    }

    // First index in a[from..] that sorts after key
    private static int upperBound(Row[] a, int from, Row key, Comparator<Row> c) {
        int lo = from, hi = a.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (c.compare(a[mid], key) <= 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    private static <T> Comparator<Row> rowComparator(List<SortKey<? super T>> keys) {
        final int k = keys.size();
        final Comparator<Object>[] comparators = keys.stream().map(key -> key.comparator).toArray(Comparator[]::new);
        return (a, b) -> {
            for (int i = 0; i < k; i++) {
                int r = comparators[i].compare(a.keys[i], b.keys[i]);
                if (r != 0) return r;
            }
            return 0;
        };
    }
}
