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
package org.pikatimer.timing.reader;

import java.io.IOException;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javafx.animation.Animation;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.IntegerProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.layout.Pane;
import org.pikatimer.timing.RawTimeData;
import org.pikatimer.timing.TimingListener;
import org.pikatimer.timing.TimingLocationInput;
import org.pikatimer.timing.TimingReader;
import org.pikatimer.util.DurationFormatter;
import org.pikatimer.util.HTTPServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pikatimer.timing.reader.ReaderUtil.runOnFX;

/**
 * Automatic select timer: phones and tablets watch the finish line with their
 * camera, read the bibs with on-device OCR, and send the bib and time here.
 *
 * The devices load a web app from PikaTimer's own web server (HTTPS, see
 * SelectTimerService) using a link that holds this input's token. All of the
 * devices using the link feed this one input. The devices queue their reads
 * and resend them until they are acknowledged, so a resend is normal and the
 * duplicate is dropped by the timing input (same bib, same time).
 *
 * The reads are bibs, not chips, so they skip the chip to bib map.
 *
 * @author John Garner
 */
public class SelectTimerReader implements TimingReader {
    private static final Logger logger = LoggerFactory.getLogger(SelectTimerReader.class);

    // Attribute keys (stored with the timing location input)
    static final String ATTR_TOKEN = "SelectTimer:token";
    private static final String ATTR_ENABLED = "SelectTimer:enabled";
    private static final String ATTR_KNOWN_BIBS = "SelectTimer:knownBibsOnly";
    private static final String ATTR_REPEAT = "SelectTimer:repeatSeconds";

    static final int DEFAULT_REPEAT_SECONDS = 60;
    static final int MAX_REPEAT_SECONDS = 3600;

    // Check the network addresses this often (seconds)
    private static final int ADDRESS_CHECK_SECONDS = 10;

    // No look alike characters (0/o, 1/l/i), since the link may be typed in
    private static final String TOKEN_CHARS = "abcdefghjkmnpqrstuvwxyz23456789";
    private static final int TOKEN_LENGTH = 8;
    private static final SecureRandom RANDOM = new SecureRandom();

    /** What happened to a read sent in by a device */
    enum ReadStatus { OK, DUPLICATE, BEFORE_EVENT, DISABLED, INVALID }

    private TimingListener timingListener;

    // Settings (FX thread)
    private final StringProperty tokenProperty = new SimpleStringProperty("");
    private final BooleanProperty enabledProperty = new SimpleBooleanProperty(false);
    private final BooleanProperty knownBibsOnlyProperty = new SimpleBooleanProperty(true);
    private final IntegerProperty repeatSecondsProperty = new SimpleIntegerProperty(DEFAULT_REPEAT_SECONDS);

    // State (FX thread)
    private final BooleanProperty activeProperty = new SimpleBooleanProperty(false);
    private final BooleanProperty readingProperty = new SimpleBooleanProperty(false);
    private final StringProperty lastReadProperty = new SimpleStringProperty("<No Reads>");
    private final IntegerProperty readCountProperty = new SimpleIntegerProperty(0);
    private final ObservableList<String> urls = FXCollections.observableArrayList();
    private final ObservableList<DeviceRow> devices = FXCollections.observableArrayList();
    private final StringProperty fingerprintProperty = new SimpleStringProperty("");

    // Copies of the settings for the web server threads
    private volatile String token = "";
    private volatile boolean enabled = false;
    private volatile boolean active = false;
    private volatile boolean knownBibsOnly = true;
    private volatile int repeatSeconds = DEFAULT_REPEAT_SECONDS;

    // Written by the web server threads, pushed to the properties by the timeline
    private volatile String lastRead = null;
    private final AtomicInteger readCount = new AtomicInteger();
    private final ConcurrentHashMap<String, DeviceStats> deviceStats = new ConcurrentHashMap<>();

    private volatile List<String> addresses = List.of();
    private final AtomicBoolean addressCheckRunning = new AtomicBoolean(false);
    private Timeline timeline;
    private int tick = 0;

    // UI
    private Parent controlRoot = null;
    private Pane displayPane = null;

    /** What we know about a device that has called in (web server threads) */
    static final class DeviceStats {
        final String id;
        volatile String name = "";
        volatile String address = "";
        volatile Instant lastContact = Instant.now();
        final AtomicInteger reads = new AtomicInteger();
        volatile int pending = 0;
        volatile Long clockOffsetMs = null;
        volatile Long rttMs = null;
        volatile String version = "";

        DeviceStats(String id) {
            this.id = id;
        }
    }

    /** A row in the device table (FX thread) */
    public record DeviceRow(String name, String address, String lastContact, int reads, int pending, String clock) {
    }

    public SelectTimerReader() {
        readingProperty.bind(activeProperty.and(enabledProperty));
    }

    // ------------------------------------------------------------------
    // TimingReader
    // ------------------------------------------------------------------

    @Override
    public void setTimingListener(TimingListener t) {
        timingListener = t;

        String savedToken = Objects.toString(t.getAttribute(ATTR_TOKEN), "").trim();
        boolean savedEnabled = !"false".equalsIgnoreCase(t.getAttribute(ATTR_ENABLED));
        boolean savedKnownBibs = !"false".equalsIgnoreCase(t.getAttribute(ATTR_KNOWN_BIBS));
        int savedRepeat = parseRepeat(t.getAttribute(ATTR_REPEAT));

        logger.debug("SelectTimer: settings for {}: enabled={} knownBibsOnly={} repeat={}",
                t.getLocationName(), savedEnabled, savedKnownBibs, savedRepeat);

        runOnFX(() -> {
            String tok = savedToken;
            if (!tok.matches("[a-z0-9]{4,32}")) {
                tok = newToken();
                setAttribute(ATTR_TOKEN, tok);
            }
            setToken(tok);
            setEnabled(savedEnabled, false);
            knownBibsOnlyProperty.set(savedKnownBibs);
            knownBibsOnly = savedKnownBibs;
            repeatSecondsProperty.set(savedRepeat);
            repeatSeconds = savedRepeat;
            activate();
        });
    }

    @Override
    public void showControls(Pane p) {
        if (controlRoot == null) {
            try {
                FXMLLoader loader = new FXMLLoader(SelectTimerReader.class.getResource("SelectTimerReader.fxml"));
                controlRoot = loader.load();
                SelectTimerReaderController controller = loader.getController();
                controller.setReader(this);
            } catch (IOException ex) {
                logger.error("Unable to load the select timer controls", ex);
                return;
            }
        }

        // If we were previously visible, clear the old one
        if (displayPane != null) displayPane.getChildren().clear();

        displayPane = p;
        displayPane.getChildren().clear();
        displayPane.getChildren().add(controlRoot);
    }

    @Override
    public void readOnce() {
        // noop
    }

    @Override
    public void startReading() {
        runOnFX(() -> {
            if (timingListener != null) activate();
        });
    }

    /**
     * Called when the input is no longer needed (type change, removal). The
     * link stops working. This does not change the saved enabled setting.
     */
    @Override
    public void stopReading() {
        runOnFX(this::deactivate);
    }

    /**
     * Clear Reads: keep accepting reads, just reset the last read display.
     */
    @Override
    public void readsCleared() {
        lastRead = null;
        runOnFX(() -> lastReadProperty.set("<No Reads>"));
    }

    @Override
    public BooleanProperty getReadingStatus() {
        return readingProperty;
    }

    @Override
    public Boolean chipIsBib() {
        return Boolean.TRUE;
    }

    // ------------------------------------------------------------------
    // Settings (FX thread)
    // ------------------------------------------------------------------

    static String newToken() {
        StringBuilder sb = new StringBuilder(TOKEN_LENGTH);
        for (int i = 0; i < TOKEN_LENGTH; i++) sb.append(TOKEN_CHARS.charAt(RANDOM.nextInt(TOKEN_CHARS.length())));
        return sb.toString();
    }

    private static int parseRepeat(String value) {
        try {
            int seconds = Integer.parseInt(Objects.toString(value, "").trim());
            return Math.max(0, Math.min(seconds, MAX_REPEAT_SECONDS));
        } catch (NumberFormatException ex) {
            return DEFAULT_REPEAT_SECONDS;
        }
    }

    private void setAttribute(String key, String value) {
        if (timingListener != null) timingListener.setAttribute(key, value);
    }

    private void setToken(String tok) {
        if (active) SelectTimerService.getInstance().unregister(token, this);
        token = tok;
        tokenProperty.set(tok);
        if (active) SelectTimerService.getInstance().register(token, this);
        updateUrls();
    }

    private void setEnabled(boolean e, boolean save) {
        enabled = e;
        enabledProperty.set(e);
        if (save) setAttribute(ATTR_ENABLED, String.valueOf(e));
    }

    private void activate() {
        if (active) return;
        active = true;
        activeProperty.set(true);
        SelectTimerService.getInstance().register(token, this);
        startTimeline();
        checkAddresses();
    }

    private void deactivate() {
        if (!active) return;
        active = false;
        activeProperty.set(false);
        SelectTimerService.getInstance().unregister(token, this);
        if (timeline != null) timeline.stop();
    }

    /** The Stop / Start Accepting Reads button. Saved with the input. */
    void toggleEnabled() {
        setEnabled(!enabledProperty.get(), true);
    }

    /** New Link: the old link (and every device using it) stops working */
    void regenerateToken() {
        String tok = newToken();
        setAttribute(ATTR_TOKEN, tok);
        setToken(tok);
        deviceStats.clear();
        devices.clear();
    }

    void setKnownBibsOnly(boolean known) {
        if (known == knownBibsOnly) return;
        knownBibsOnly = known;
        knownBibsOnlyProperty.set(known);
        setAttribute(ATTR_KNOWN_BIBS, String.valueOf(known));
    }

    void setRepeatSeconds(int seconds) {
        int s = Math.max(0, Math.min(seconds, MAX_REPEAT_SECONDS));
        if (s == repeatSeconds) return;
        repeatSeconds = s;
        repeatSecondsProperty.set(s);
        setAttribute(ATTR_REPEAT, String.valueOf(s));
    }

    // ------------------------------------------------------------------
    // Timeline (FX thread)
    // ------------------------------------------------------------------

    private void startTimeline() {
        if (timeline != null) timeline.stop();
        tick = 0;
        timeline = new Timeline(new KeyFrame(javafx.util.Duration.seconds(1), event -> onTick()));
        timeline.setCycleCount(Animation.INDEFINITE);
        timeline.play();
    }

    // Once a second: push the web server updates to the properties
    private void onTick() {
        String r = lastRead;
        if (r != null && !r.equals(lastReadProperty.get())) lastReadProperty.set(r);
        readCountProperty.set(readCount.get());

        Instant now = Instant.now();
        List<DeviceRow> rows = new ArrayList<>();
        deviceStats.values().stream()
                .sorted(Comparator.comparing((DeviceStats d) -> d.name.toLowerCase()).thenComparing(d -> d.id))
                .forEach(d -> rows.add(new DeviceRow(
                        d.name.isBlank() ? "Device " + d.id.substring(0, Math.min(6, d.id.length())) : d.name,
                        d.address,
                        ago(Duration.between(d.lastContact, now)),
                        d.reads.get(),
                        d.pending,
                        formatClock(d.clockOffsetMs, d.rttMs))));
        if (!rows.equals(devices)) devices.setAll(rows);

        if (++tick % ADDRESS_CHECK_SECONDS == 0) checkAddresses();
    }

    private static String ago(Duration d) {
        long s = Math.max(0, d.toSeconds());
        if (s < 2) return "now";
        if (s < 120) return s + " s ago";
        if (s < 7200) return (s / 60) + " min ago";
        return (s / 3600) + " h ago";
    }

    private static String formatClock(Long offsetMs, Long rttMs) {
        if (offsetMs == null) return "";
        // + means the device's clock is ahead of this computer
        String text = String.format("%+.2f s", -offsetMs / 1000.0);
        if (rttMs != null) text += " (±" + Math.max(1, rttMs / 2) + " ms)";
        return text;
    }

    // Look up the network addresses off the FX thread (it can be slow), and
    // make sure the HTTPS certificate covers them
    private void checkAddresses() {
        if (!addressCheckRunning.compareAndSet(false, true)) return;
        CompletableFuture.supplyAsync(() -> {
            List<String> found = HTTPServices.getLocalIPv4Addresses();
            HTTPServices.getInstance().refreshCertificate();
            return found;
        }).whenComplete((found, ex) -> {
            addressCheckRunning.set(false);
            if (ex != null) {
                logger.debug("Unable to check the network addresses", ex);
                return;
            }
            runOnFX(() -> {
                fingerprintProperty.set(HTTPServices.getInstance().getCertificateFingerprint());
                if (!found.equals(addresses)) {
                    addresses = found;
                    updateUrls();
                }
            });
        });
    }

    private void updateUrls() {
        HTTPServices http = HTTPServices.getInstance();
        int httpsPort = http.httpsPort();
        List<String> list = new ArrayList<>();
        for (String ip : addresses) {
            if (httpsPort > 0) list.add("https://" + ip + ":" + httpsPort + "/select/" + token + "/");
            else list.add("http://" + ip + ":" + http.port() + "/select/" + token + "/");
        }
        if (!list.equals(urls)) urls.setAll(list);
    }

    // ------------------------------------------------------------------
    // Web server side (called on the web server's threads)
    // ------------------------------------------------------------------

    boolean isEnabled() {
        return active && enabled;
    }

    boolean isKnownBibsOnly() {
        return knownBibsOnly;
    }

    int getRepeatSeconds() {
        return repeatSeconds;
    }

    String getToken() {
        return token;
    }

    String getInputName() {
        return timingListener == null ? "" : timingListener.getLocationName();
    }

    String getTimingLocationName() {
        if (timingListener instanceof TimingLocationInput tli && tli.getTimingLocation() != null) {
            return tli.getTimingLocation().getLocationName();
        }
        return "";
    }

    /**
     * Note that a device called in
     */
    DeviceStats deviceContact(String id, String name, String address, Integer pending, Long offsetMs, Long rttMs, String version) {
        if (id == null || id.isBlank()) return null;
        DeviceStats d = deviceStats.computeIfAbsent(id, DeviceStats::new);
        d.lastContact = Instant.now();
        if (name != null && !name.isBlank()) d.name = name;
        if (address != null) d.address = address;
        if (pending != null) d.pending = pending;
        if (offsetMs != null) d.clockOffsetMs = offsetMs;
        if (rttMs != null) d.rttMs = rttMs;
        if (version != null) d.version = version;
        return d;
    }

    /**
     * A bib seen by a device.
     *
     * @param device the device that saw it (may be null)
     * @param bib the bib
     * @param epochMs when it was seen, already corrected to this computer's clock
     * @return what happened to it
     */
    ReadStatus acceptRead(DeviceStats device, String bib, long epochMs) {
        TimingListener listener = timingListener;
        if (listener == null || !isEnabled()) return ReadStatus.DISABLED;
        if (bib == null || bib.isBlank()) return ReadStatus.INVALID;

        LocalDate eventDate = listener.getEventDate();
        LocalDateTime seen = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMs), ZoneId.systemDefault());
        Duration time = Duration.between(eventDate.atStartOfDay(), seen);
        if (time.isNegative()) {
            logger.debug("SelectTimer: dropping read for {} at {}, it is before the event date", bib, seen);
            return ReadStatus.BEFORE_EVENT;
        }

        RawTimeData rawTime = new RawTimeData();
        rawTime.setChip(bib);
        rawTime.setTimestampLong(time.toNanos());

        // The timing input drops duplicates on its own. Check first so the
        // device can be told (a resend after a network drop is normal).
        boolean duplicate = false;
        if (listener instanceof TimingLocationInput tli) {
            rawTime.setTimingLocationInputId(tli.getID());
            duplicate = listener.getReads().contains(rawTime);
        }
        listener.processRead(rawTime);
        if (duplicate) return ReadStatus.DUPLICATE;

        readCount.incrementAndGet();
        if (device != null) device.reads.incrementAndGet();
        String from = device == null || device.name.isBlank() ? "" : " (" + device.name + ")";
        lastRead = bib + " @ " + DurationFormatter.durationToString(time, 1) + from;
        return ReadStatus.OK;
    }

    // ------------------------------------------------------------------
    // Properties for the controls
    // ------------------------------------------------------------------

    StringProperty tokenProperty() {
        return tokenProperty;
    }

    BooleanProperty enabledProperty() {
        return enabledProperty;
    }

    BooleanProperty activeProperty() {
        return activeProperty;
    }

    BooleanProperty knownBibsOnlyProperty() {
        return knownBibsOnlyProperty;
    }

    IntegerProperty repeatSecondsProperty() {
        return repeatSecondsProperty;
    }

    StringProperty lastReadProperty() {
        return lastReadProperty;
    }

    IntegerProperty readCountProperty() {
        return readCountProperty;
    }

    StringProperty fingerprintProperty() {
        return fingerprintProperty;
    }

    ObservableList<String> getUrls() {
        return urls;
    }

    ObservableList<DeviceRow> getDevices() {
        return devices;
    }
}
