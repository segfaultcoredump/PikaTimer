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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import javafx.collections.ObservableList;

/**
 * Helpers for applying large changes to an ObservableList that feeds a
 * FilteredList / SortedList / TableView chain.
 *
 * A SortedList re-sorts the whole list for every "updated" change and walks
 * its whole index for every single element that is added or removed. The only
 * cheap path is replacing the entire content in one change (setAll), which
 * sorts once. These helpers pick the incremental path for small changes (so
 * the table selection survives) and the single replace for large ones.
 *
 * All methods must be called on the JavaFX application thread.
 */
public final class FxBulk {

    /** More element updates than this in one batch are cheaper as one replace */
    public static final int UPDATE_THRESHOLD = 20;

    /** More adds/removes than this in one batch are cheaper as one replace */
    public static final int ADD_REMOVE_THRESHOLD = 500;

    private FxBulk() {}

    public static <T> void addAll(ObservableList<T> list, Collection<? extends T> items) {
        if (items.isEmpty()) return;
        // Adding to an empty list is already a single sort
        if (list.isEmpty() || items.size() <= ADD_REMOVE_THRESHOLD) {
            list.addAll(items);
        } else {
            List<T> next = new ArrayList<>(list.size() + items.size());
            next.addAll(list);
            next.addAll(items);
            list.setAll(next);
        }
    }

    public static <T> void removeIf(ObservableList<T> list, Predicate<? super T> remove) {
        int matches = 0;
        for (T t: list) if (remove.test(t)) matches++;
        if (matches == 0) return;
        if (matches == list.size()) {
            list.clear();
        } else if (matches <= ADD_REMOVE_THRESHOLD) {
            list.removeIf(remove);
        } else {
            replace(list, remove, List.of());
        }
    }

    /**
     * Drop everything matching the remove predicate, append the new items, and
     * publish the result as one single change no matter how large it is.
     */
    public static <T> void replace(ObservableList<T> list, Predicate<? super T> remove, Collection<? extends T> add) {
        List<T> next = new ArrayList<>(list.size() + add.size());
        for (T t: list) if (!remove.test(t)) next.add(t);
        next.addAll(add);
        list.setAll(next);
    }
}
