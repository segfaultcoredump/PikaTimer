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
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import javafx.animation.Animation;
import javafx.animation.KeyFrame;
import javafx.animation.PauseTransition;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.beans.InvalidationListener;
import javafx.beans.binding.Bindings;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.DoubleProperty;
import javafx.beans.property.IntegerProperty;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleDoubleProperty;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import javafx.fxml.FXMLLoader;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.ButtonBar;
import javafx.scene.control.ButtonType;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.DatePicker;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.ProgressBar;
import javafx.scene.control.Separator;
import javafx.scene.control.TextField;
import javafx.scene.control.TextFormatter;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.Screen;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.pikatimer.event.Event;
import org.pikatimer.timing.RawTimeData;
import org.pikatimer.timing.TimingListener;
import org.pikatimer.timing.TimingReader;
import org.pikatimer.util.DurationFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pikatimer.timing.reader.ReaderUtil.errorMessage;
import static org.pikatimer.timing.reader.ReaderUtil.installTimeFilter;
import static org.pikatimer.timing.reader.ReaderUtil.runOnFX;
import static org.pikatimer.timing.reader.ReaderUtil.setStatus;
import static org.pikatimer.timing.reader.ReaderUtil.toDateTime;

/**
 * Connects a timing input to a PikaReader unit.
 *
 * Endpoints used:
 * ws(s)://host:port/events -> READ and STATUS events
 * GET /start, GET /stop -> start / stop the RFID readers
 * GET /status -> status (used to check a manually entered address)
 * GET /profiles, PUT /profile/{id} -> reader profiles
 * GET /rewind/{from}/{to} -> reads between two local date/times
 *
 * Discovery: UDP broadcast of DISCOVER_PIKA_READER_REQUEST to port 8888,
 * answered with "PIKA_READER unitID port".
 *
 * @author John Garner
 */
public class PikaReaderDirectReader implements TimingReader {
    private static final Logger logger = LoggerFactory.getLogger(PikaReaderDirectReader.class);

    // Attribute keys (stored with the timing location input)
    private static final String ATTR_IP = "PikaReaderDirect:pika_ip";
    private static final String ATTR_PORT = "PikaReaderDirect:pika_port";
    private static final String ATTR_SECURE = "PikaReaderDirect:secure";
    private static final String ATTR_UNIT_ID = "PikaReaderDirect:unit_id";
    private static final String ATTR_CONNECT = "PikaReaderDirect:connect";

    // "2023-12-28 13:45:24.476" (the fraction is optional and 0-9 digits)
    private static final DateTimeFormatter tagReadFormatter = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .toFormatter();

    // One shared thread for all of the reconnect attempts
    private static final ScheduledExecutorService RECONNECT = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        thread.setName("PikaReader-reconnect");
        return thread;
    });

    private static final HttpClient httpClient = HttpClient.newHttpClient();

    // Seconds since the last STATUS, capped. Also used as the "not heard from" value
    static final int LAST_UPDATED_MAX = 99;

    private TimingListener timingListener;

    // Connection settings
    private final StringProperty readerIPProperty = new SimpleStringProperty("");
    private final StringProperty readerPortProperty = new SimpleStringProperty("");
    private final BooleanProperty secureProperty = new SimpleBooleanProperty(false);
    private final BooleanProperty configuredProperty = new SimpleBooleanProperty(false);

    // Reader state (only updated on the FX thread)
    private final BooleanProperty readingProperty = new SimpleBooleanProperty(false);
    private final BooleanProperty connectedProperty = new SimpleBooleanProperty(false);
    private final BooleanProperty enabledProperty = new SimpleBooleanProperty(false);
    private final DoubleProperty batteryProperty = new SimpleDoubleProperty(-2);
    private final StringProperty rfidReaderProblemProperty = new SimpleStringProperty("");
    private final StringProperty readerNameProperty = new SimpleStringProperty("");
    private final StringProperty lastReadProperty = new SimpleStringProperty("<No Reads>");
    private final IntegerProperty lastUpdatedProperty = new SimpleIntegerProperty(LAST_UPDATED_MAX);

    // Written by the websocket and rewind threads, pushed to the properties by the timeline
    private volatile LocalDateTime lastUpdated = null;
    private volatile String lastRead = null;

    // Reader profiles (PikaReader 6684b90 and newer).
    // Older PikaReader versions do not send a profile in their STATUS messages,
    // so profileSupportedProperty stays false and the UI hides the profile.
    private final BooleanProperty profileSupportedProperty = new SimpleBooleanProperty(false);
    private final StringProperty profileIDProperty = new SimpleStringProperty("");
    private final StringProperty profileNameProperty = new SimpleStringProperty("");
    private final ObservableList<Profile> profiles = FXCollections.observableArrayList();
    private final AtomicBoolean profilesLoading = new AtomicBoolean(false);
    private volatile boolean profilesLoaded = false;

    // True while a connection attempt is in progress
    private final AtomicBoolean connecting = new AtomicBoolean(false);
    private volatile EventWebSocketClient webSocketClient;
    private Timeline timeline;

    // UI
    private Parent controlRoot = null;
    private PikaReaderDirectReaderController controller = null;
    private Pane displayPane = null;

    public PikaReaderDirectReader() {
    }

    // ------------------------------------------------------------------
    // TimingReader
    // ------------------------------------------------------------------

    @Override
    public void setTimingListener(TimingListener t) {
        timingListener = t;

        String ip = Objects.toString(t.getAttribute(ATTR_IP), "").trim();
        String port = Objects.toString(t.getAttribute(ATTR_PORT), "").trim();
        boolean secure = Boolean.parseBoolean(t.getAttribute(ATTR_SECURE));
        String unitID = Objects.toString(t.getAttribute(ATTR_UNIT_ID), "").trim();
        boolean autoConnect = !"false".equalsIgnoreCase(t.getAttribute(ATTR_CONNECT));

        logger.debug("PikaReaderDirect: settings for {}: ip={} port={} secure={} unit={} connect={}",
                t.getLocationName(), ip, port, secure, unitID, autoConnect);

        runOnFX(() -> {
            setAddress(ip, port, secure, unitID);
            if (configuredProperty.get() && autoConnect) enable();
        });
    }

    @Override
    public void showControls(Pane p) {
        if (controlRoot == null) {
            try {
                FXMLLoader loader = new FXMLLoader(PikaReaderDirectReader.class.getResource("PikaReaderDirectReader.fxml"));
                controlRoot = loader.load();
                controller = loader.getController();
                controller.setReader(this);
            } catch (IOException ex) {
                logger.error("Unable to load the PikaReader controls", ex);
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
        // Connect to the unit. Starting the unit's RFID readers is only done
        // from the Start Reader button.
        runOnFX(() -> {
            if (configuredProperty.get()) enable();
        });
    }

    /**
     * Called when the input is no longer needed (type change, removal, clearing
     * the reads). This only disconnects from the unit, it never stops the unit,
     * and it does not change the saved auto connect setting.
     */
    @Override
    public void stopReading() {
        runOnFX(this::disable);
    }

    /**
     * Clear Reads: the reads have already been removed by the timing input,
     * so stay connected and just reset the last read display.
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
        return Boolean.FALSE;
    }

    // ------------------------------------------------------------------
    // Connection
    // ------------------------------------------------------------------

    // Must be called on the FX thread
    private void setAddress(String ip, String port, boolean secure, String unitID) {
        readerIPProperty.set(ip);
        readerPortProperty.set(port);
        secureProperty.set(secure);
        readerNameProperty.set(unitID.isBlank() ? ip : unitID);
        configuredProperty.set(!ip.isBlank() && !port.isBlank());
    }

    // Must be called on the FX thread
    private void resetState() {
        readingProperty.set(false);
        batteryProperty.set(-2);
        rfidReaderProblemProperty.set("");
        profileSupportedProperty.set(false);
        profileIDProperty.set("");
        profileNameProperty.set("");
        profiles.clear();
        profilesLoaded = false;
        lastUpdated = null;
        lastUpdatedProperty.set(LAST_UPDATED_MAX);
        lastRead = null;
        lastReadProperty.set("<No Reads>");
    }

    // Must be called on the FX thread
    private void connect() {
        if (!configuredProperty.get()) return;
        if (connectedProperty.get()) return; // already connected
        if (!connecting.compareAndSet(false, true)) return; // already trying

        // periodically push the background updates to the properties
        if (timeline != null) timeline.stop();
        timeline = new Timeline(new KeyFrame(javafx.util.Duration.seconds(1), event -> {
            String r = lastRead;
            if (r != null && !r.equals(lastReadProperty.get())) lastReadProperty.set(r);

            LocalDateTime updated = lastUpdated;
            long seconds = updated == null ? LAST_UPDATED_MAX : Duration.between(updated, LocalDateTime.now()).toSeconds();
            lastUpdatedProperty.set((int) Math.max(0, Math.min(seconds, LAST_UPDATED_MAX)));
        }));
        timeline.setCycleCount(Animation.INDEFINITE);
        timeline.play();

        String wsURL = (secureProperty.get() ? "wss://" : "ws://") + uriHost(readerIPProperty.getValueSafe())
                + ":" + readerPortProperty.getValueSafe() + "/events";
        String name = readerNameProperty.getValueSafe();

        Task<Void> connection = new Task<>() {
            @Override
            public Void call() {
                try {
                    logger.debug("Connecting to {}", wsURL);
                    EventWebSocketClient client = new EventWebSocketClient(wsURL);
                    webSocketClient = client;
                    client.setDaemon(true);
                    client.connectBlocking(10, TimeUnit.SECONDS);

                    Platform.runLater(() -> {
                        if (client == webSocketClient && client.isOpen()) {
                            connectedProperty.set(true);
                        }
                    });
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                } catch (Exception ex) {
                    logger.error("PikaReader connection error: {} {}", name, ex.getMessage(), ex);
                } finally {
                    connecting.set(false);
                }
                return null;
            }
        };
        Thread connectionThread = new Thread(connection);
        connectionThread.setName("Thread-PikaReader-" + readerIPProperty.getValueSafe());
        connectionThread.setDaemon(true);
        connectionThread.start();
    }

    // Must be called on the FX thread
    void enable() {
        enabledProperty.set(true);
        connect();
    }

    // Must be called on the FX thread
    void disable() {
        enabledProperty.set(false);
        // The client is created by the connection thread, so it may not exist yet
        EventWebSocketClient client = webSocketClient;
        if (client != null) client.close();
        if (timeline != null) timeline.stop();
        lastUpdatedProperty.set(LAST_UPDATED_MAX);
        connectedProperty.set(false);
    }

    /**
     * The Disconnect / Reconnect button. Saves the choice so the input
     * does (or does not) connect when the event is opened again.
     */
    void toggleConnection() {
        if (enabledProperty.get()) {
            disable();
            setAttribute(ATTR_CONNECT, "false");
        } else {
            enable();
            setAttribute(ATTR_CONNECT, "true");
        }
    }

    // Switch this input over to a different unit (FX thread)
    private void useReader(String ip, String port, boolean secure, String unitID) {
        disable();
        webSocketClient = null;
        resetState();
        setAddress(ip, port, secure, unitID);
        setAttribute(ATTR_IP, ip);
        setAttribute(ATTR_PORT, port);
        setAttribute(ATTR_SECURE, Boolean.toString(secure));
        setAttribute(ATTR_UNIT_ID, unitID);
        setAttribute(ATTR_CONNECT, "true");
        enable();
    }

    private void setAttribute(String key, String value) {
        if (timingListener != null) timingListener.setAttribute(key, value);
    }

    // ------------------------------------------------------------------
    // Messages from the unit
    // ------------------------------------------------------------------

    private void processLine(String line) {
        JSONObject message;
        try {
            message = new JSONObject(line);
        } catch (JSONException ex) {
            logger.warn("Ignoring a malformed message from {}: {}", readerNameProperty.getValueSafe(), ex.getMessage());
            return;
        }
        String type = message.optString("type", "UNKNOWN");
        try {
            switch (type) {
                case "READ" -> processRead(message, false);
                case "STATUS" -> processStatus(message);
                default -> logger.debug("Unknown message from {}: {}", readerNameProperty.getValueSafe(), line);
            }
        } catch (RuntimeException ex) {
            logger.warn("Unable to process a {} message from {}: {}", type, readerNameProperty.getValueSafe(), line, ex);
        }
    }

    private void processStatus(JSONObject status) {
        logger.trace("PikaReader Status: {} -> {}", readerNameProperty.getValueSafe(), status);

        // Pull everything out of the message here, only update the properties on the FX thread
        String unitID = status.optString("unitID", "");
        double battery = status.optDouble("battery", -1);
        boolean hasReading = status.has("reading");
        boolean reading = status.optBoolean("reading", false);
        JSONObject profile = status.optJSONObject("profile");
        String rfidProblem = rfidReaderProblem(status.optJSONArray("readers"));

        Platform.runLater(() -> {
            if (!unitID.isBlank()) readerNameProperty.set(unitID);

            batteryProperty.set(battery > 0 ? battery / 100.0 : -1);

            if (hasReading) readingProperty.set(reading);

            rfidReaderProblemProperty.set(rfidProblem);

            // {"profile":{"id":"finish_mats","name":"Finish Mats"}}
            if (profile != null) {
                String id = profile.optString("id");
                String name = profile.optString("name", id);
                profileSupportedProperty.set(true);
                profileIDProperty.set(id);
                profileNameProperty.set(name);

                // If we don't know about this profile yet (first status or
                // the profile list changed on the unit), add a placeholder
                // so it can be displayed and then go fetch the full list.
                // Keep trying to fetch the list until it loads.
                boolean unknownProfile = !id.isBlank() && profiles.stream().noneMatch(p -> p.id().equals(id));
                if (unknownProfile) profiles.add(new Profile(id, name, ""));
                if (unknownProfile || !profilesLoaded) loadProfiles();
            } else {
                profileSupportedProperty.set(false);
            }
        });

        lastUpdated = LocalDateTime.now();
    }

    // {"readers":[{"id":1,"name":"Reader 1","connected":true,...},...]}
    // Returns "Reader 1 not connected" (or "Reader 1, Reader 2 not connected"),
    // or "" if every RFID reader is connected or the unit didn't list them.
    // A reader without a "connected" flag is assumed to be fine.
    static String rfidReaderProblem(JSONArray readers) {
        if (readers == null) return "";
        List<String> down = new ArrayList<>();
        for (int i = 0; i < readers.length(); i++) {
            JSONObject r = readers.optJSONObject(i);
            if (r == null || r.optBoolean("connected", true)) continue;
            down.add(r.optString("name", "Reader " + r.optString("id", "?")));
        }
        if (down.isEmpty()) return "";
        return String.join(", ", down) + " not connected";
    }

    /**
     * Hands a read to the timing listener (which ignores duplicates).
     *
     * {"antenna":3,"chip":"23210006","rssi":-71,"reader":1,"tz":"-07:00",
     *  "epochMilli":1703796324476,"type":"READ","timestamp":"2023-12-28 13:45:24.476"}
     *
     * @return true if the read was sent to the timing listener
     */
    private boolean processRead(JSONObject tagRead, boolean rewind) {
        logger.trace("Chip read from {}: {}", readerNameProperty.getValueSafe(), tagRead);

        TimingListener listener = timingListener;
        if (listener == null) return false;

        String chip = tagRead.optString("chip", "").trim();
        String timestamp = tagRead.optString("timestamp", "").trim();
        if (chip.isEmpty() || timestamp.isEmpty()) {
            logger.debug("Skipping a read without a chip or timestamp from {}: {}", readerNameProperty.getValueSafe(), tagRead);
            return false;
        }

        LocalDateTime readTime;
        try {
            readTime = LocalDateTime.parse(timestamp, tagReadFormatter);
        } catch (DateTimeParseException ex) {
            logger.debug("Skipping a read with a bad timestamp from {}: {}", readerNameProperty.getValueSafe(), tagRead);
            return false;
        }

        Duration time = Duration.between(Event.getInstance().getLocalEventDate().atStartOfDay(), readTime);
        if (time.isNegative()) {
            logger.debug("Read of {} at {} is before the event date, ignoring", chip, timestamp);
            return false;
        }

        RawTimeData rawTime = new RawTimeData();
        rawTime.setChip(chip);
        rawTime.setTimestampLong(time.toNanos());
        listener.processRead(rawTime);

        lastRead = chip + " @ " + DurationFormatter.durationToString(time, 3);
        return true;
    }

    // ------------------------------------------------------------------
    // Commands
    // ------------------------------------------------------------------

    // Completes exceptionally (with the reader's error message) if the
    // reader can't be reached or rejects the command
    CompletableFuture<HttpResponse<String>> startReader() {
        return sendCommand("/start");
    }

    CompletableFuture<HttpResponse<String>> stopReader() {
        return sendCommand("/stop");
    }

    private CompletableFuture<HttpResponse<String>> sendCommand(String command) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(getBaseURL() + command))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        logger.debug("Sending {} to {}", command, readerNameProperty.getValueSafe());
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> {
                    if (response.statusCode() < 200 || response.statusCode() > 299) {
                        throw new RuntimeException(responseError(response));
                    }
                    return response;
                });
    }

    // The reader's web address, e.g. http://192.168.1.10:8080
    String getBaseURL() {
        return baseURL(secureProperty.get(), readerIPProperty.getValueSafe(), readerPortProperty.getValueSafe());
    }

    private static String baseURL(boolean secure, String host, String port) {
        return (secure ? "https://" : "http://") + uriHost(host) + ":" + port;
    }

    // IPv6 literals need [brackets] in a URL
    static String uriHost(String host) {
        if (host.contains(":") && !host.startsWith("[")) return "[" + host + "]";
        return host;
    }

    /**
     * Checks that a PikaReader answers at an address that was entered by hand.
     *
     * @return completes with the unit ID from GET /status, or exceptionally
     * if nothing answers or the response is not from a PikaReader
     */
    static CompletableFuture<String> probeStatus(boolean secure, String host, int port) {
        HttpRequest request;
        try {
            request = HttpRequest.newBuilder()
                    .uri(URI.create(baseURL(secure, host, String.valueOf(port)) + "/status"))
                    .timeout(Duration.ofSeconds(3))
                    .GET()
                    .build();
        } catch (IllegalArgumentException ex) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Invalid address"));
        }
        logger.debug("Checking for a PikaReader at {}", request.uri());
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                // The request timeout may not cover the connect, so back it up
                .orTimeout(5, TimeUnit.SECONDS)
                .thenApply(response -> {
                    // A PikaReader always answers /status
                    if (response.statusCode() < 200 || response.statusCode() > 299) {
                        throw new RuntimeException("Not a PikaReader (" + responseError(response) + ")");
                    }
                    String unitID;
                    try {
                        unitID = new JSONObject(response.body()).optString("unitID", "");
                    } catch (JSONException ex) {
                        unitID = "";
                    }
                    if (unitID.isBlank()) throw new RuntimeException("Not a PikaReader (no unitID in /status)");
                    return unitID;
                });
    }

    // The "error" from a PikaReader error response, or the HTTP status code
    private static String responseError(HttpResponse<String> response) {
        try {
            return new JSONObject(response.body()).optString("error", "HTTP " + response.statusCode());
        } catch (JSONException ex) {
            return "HTTP " + response.statusCode();
        }
    }

    // ------------------------------------------------------------------
    // Profiles
    // ------------------------------------------------------------------

    // Fetch the list of available profiles from the reader
    // {"current":"finish_mats","profiles":[{"id":"finish_mats","name":"Finish Mats","description":"...","tagPopulation":32},...]}
    void loadProfiles() {
        // The STATUS messages arrive once a second, so don't stack up requests
        if (!profilesLoading.compareAndSet(false, true)) return;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(getBaseURL() + "/profiles"))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        String name = readerNameProperty.getValueSafe();

        httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenAccept(response -> {
                    if (response.statusCode() < 200 || response.statusCode() > 299) {
                        logger.debug("Unable to load the profiles for {}: HTTP {}", name, response.statusCode());
                        return;
                    }
                    JSONArray list = new JSONObject(response.body()).getJSONArray("profiles");
                    List<Profile> newProfiles = new ArrayList<>();
                    for (int i = 0; i < list.length(); i++) {
                        JSONObject p = list.getJSONObject(i);
                        String id = p.getString("id");
                        newProfiles.add(new Profile(id, p.optString("name", id), p.optString("description", "")));
                    }
                    logger.debug("Loaded {} profiles for {}", newProfiles.size(), name);
                    Platform.runLater(() -> {
                        // Keep the current profile in the list even if the unit didn't return it
                        String currentID = profileIDProperty.getValueSafe();
                        if (!currentID.isBlank() && newProfiles.stream().noneMatch(p -> p.id().equals(currentID))) {
                            newProfiles.add(new Profile(currentID, profileNameProperty.getValueSafe(), ""));
                        }
                        profiles.setAll(newProfiles);
                    });
                    profilesLoaded = true;
                })
                .exceptionally(ex -> {
                    logger.debug("Unable to load the profiles for {}: {}", name, ex.getMessage());
                    return null;
                })
                .whenComplete((v, ex) -> profilesLoading.set(false));
    }

    // Change the reader profile. Only allowed by the reader when it is not reading.
    // Completes with the reader's response on success, or exceptionally with
    // the reader's error message (e.g. 409 if the reader is reading).
    CompletableFuture<JSONObject> changeProfile(String id) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(getBaseURL() + "/profile/"
                        + URLEncoder.encode(id, StandardCharsets.UTF_8).replace("+", "%20")))
                .timeout(Duration.ofSeconds(45)) // The reader can take up to 30 seconds to apply the profile
                .PUT(HttpRequest.BodyPublishers.noBody())
                .build();

        String name = readerNameProperty.getValueSafe();
        logger.debug("Changing the profile for {} to {}", name, id);
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> {
                    JSONObject body;
                    try {
                        body = new JSONObject(response.body());
                    } catch (JSONException ex) {
                        body = new JSONObject();
                    }

                    if (response.statusCode() < 200 || response.statusCode() > 299) {
                        throw new RuntimeException(body.optString("error", "HTTP " + response.statusCode()));
                    }

                    String newID = body.optString("id", id);
                    String newName = body.optString("name", newID);
                    logger.debug("Profile for {} changed to {}", name, newName);
                    Platform.runLater(() -> {
                        profileIDProperty.set(newID);
                        profileNameProperty.set(newName);
                    });
                    return body;
                });
    }

    // ------------------------------------------------------------------
    // Rewind
    // ------------------------------------------------------------------

    void rewind() {
        if (!connectedProperty.get()) {
            Alert alert = new Alert(AlertType.INFORMATION);
            alert.setTitle("Rewind");
            alert.setHeaderText("Reader is not connected");
            alert.setContentText(readerNameProperty.getValueSafe() + " is not connected.\nRewind will be available once the reader reconnects.");
            alert.showAndWait();
            return;
        }

        Dialog<RewindRange> dialog = new Dialog<>();
        dialog.setTitle("Rewind");
        dialog.setHeaderText("Rewind timing data...");
        ButtonType rewindButtonType = new ButtonType("Rewind", ButtonBar.ButtonData.OK_DONE);
        dialog.getDialogPane().getButtonTypes().addAll(rewindButtonType, ButtonType.CANCEL);

        VBox rewindVBox = new VBox();
        rewindVBox.setSpacing(5.0);
        rewindVBox.setStyle("-fx-font-size: 16px;");

        HBox startHBox = new HBox();
        startHBox.setSpacing(5.0);
        Label startLabel = new Label("From:");
        startLabel.setMinWidth(40);
        DatePicker startDate = new DatePicker();
        TextField startTime = new TextField();
        startHBox.getChildren().addAll(startLabel, startDate, startTime);

        HBox endHBox = new HBox();
        endHBox.setSpacing(5.0);
        Label endLabel = new Label("To:");
        endLabel.setMinWidth(40);
        DatePicker endDate = new DatePicker();
        TextField endTime = new TextField();
        endHBox.getChildren().addAll(endLabel, endDate, endTime);

        rewindVBox.getChildren().addAll(startHBox, endHBox);
        dialog.getDialogPane().setContent(rewindVBox);

        BooleanProperty startTimeOK = new SimpleBooleanProperty(false);
        BooleanProperty endTimeOK = new SimpleBooleanProperty(false);
        BooleanProperty allOK = new SimpleBooleanProperty(false);

        installTimeFilter(startTime, startTimeOK);
        installTimeFilter(endTime, endTimeOK);

        // Both times valid, both dates set, and from is not after to
        Runnable validate = () -> {
            boolean ok = startTimeOK.get() && endTimeOK.get()
                    && startDate.getValue() != null && endDate.getValue() != null;
            if (ok) {
                LocalDateTime from = toDateTime(startDate.getValue(), startTime.getText());
                LocalDateTime to = toDateTime(endDate.getValue(), endTime.getText());
                ok = !from.isAfter(to);
            }
            allOK.set(ok);
        };
        startTime.textProperty().addListener(o -> validate.run());
        endTime.textProperty().addListener(o -> validate.run());
        startDate.valueProperty().addListener(o -> validate.run());
        endDate.valueProperty().addListener(o -> validate.run());

        // Default to the event date, 00:00:00 -> 23:59:59
        LocalDate eventDate = Event.getInstance().getLocalEventDate();
        startDate.setValue(eventDate);
        startTime.setText("00:00:00");
        endDate.setValue(eventDate);
        endTime.setText("23:59:59");

        Node rewindButton = dialog.getDialogPane().lookupButton(rewindButtonType);
        rewindButton.disableProperty().bind(allOK.not());

        dialog.setResultConverter(dialogButton -> {
            if (dialogButton == rewindButtonType) {
                return new RewindRange(
                        toDateTime(startDate.getValue(), startTime.getText()),
                        toDateTime(endDate.getValue(), endTime.getText()));
            }
            return null;
        });

        Optional<RewindRange> result = dialog.showAndWait();
        if (result.isEmpty()) return;

        RewindRange range = result.get();
        String readerName = readerNameProperty.getValueSafe();
        String from = range.from().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        String to = range.to().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        logger.info("Rewind {} from {} to {}", readerName, from, to);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(getBaseURL() + "/rewind/" + from + "/" + to))
                .timeout(Duration.ofSeconds(120)) // A full day can be a lot of reads
                .GET()
                .build();

        // Fetch and process the reads in the background, then report back
        httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(response -> {
                    if (response.statusCode() < 200 || response.statusCode() > 299) {
                        throw new RuntimeException(responseError(response));
                    }
                    JSONArray rewinds = new JSONArray(response.body());
                    int processed = 0;
                    int skipped = 0;
                    for (int i = 0; i < rewinds.length(); i++) {
                        try {
                            JSONObject obj = rewinds.optJSONObject(i);
                            if (obj != null && processRead(obj, true)) processed++;
                            else skipped++;
                        } catch (RuntimeException ex) {
                            skipped++;
                            logger.debug("Skipping a bad rewind read from {}: {}", readerName, ex.getMessage());
                        }
                    }
                    logger.info("Rewind {}: {} reads processed, {} skipped", readerName, processed, skipped);
                    return new int[]{processed, skipped};
                })
                .whenComplete((counts, ex) -> Platform.runLater(() -> {
                    if (ex != null) {
                        logger.error("Rewind of {} failed: {}", readerName, errorMessage(ex));
                        Alert alert = new Alert(AlertType.ERROR);
                        alert.setTitle("Rewind");
                        alert.setHeaderText("Unable to rewind " + readerName);
                        alert.setContentText(errorMessage(ex));
                        alert.show();
                    } else {
                        Alert alert = new Alert(AlertType.INFORMATION);
                        alert.setTitle("Rewind");
                        alert.setHeaderText("Rewind of " + readerName + " complete");
                        String msg = "Processed " + counts[0] + " reads.\nReads that were already received are ignored.";
                        if (counts[1] > 0) msg += "\n" + counts[1] + " reads were skipped (unreadable or before the event date).";
                        alert.setContentText(msg);
                        alert.show();
                    }
                }));
    }

    // ------------------------------------------------------------------
    // Discovery / reader selection
    // ------------------------------------------------------------------

    private static final String HTTP = "http://";
    private static final String HTTPS = "https://";
    // Just a host name or IP address (no scheme, path, user or spaces)
    private static final Pattern MANUAL_HOST_PATTERN = Pattern.compile("[^\\s/@?#]+");

    void discover() {
        logger.debug("Starting discover...");
        ObservableList<DiscoveredReader> readers = FXCollections.observableArrayList();
        BooleanProperty scanCompleted = new SimpleBooleanProperty(false);
        BooleanProperty dialogClosed = new SimpleBooleanProperty(false);

        // start a discovery task in a background thread
        Task<Void> pikaSearch = new Task<>() {
            @Override
            public Void call() {
                byte[] packetData = "DISCOVER_PIKA_READER_REQUEST".getBytes(StandardCharsets.UTF_8);

                // Find the units using UDP broadcast, looping while the dialog box is open
                // UDP Broadcast code borrowed from https://demey.io/network-discovery-using-udp-broadcast/
                while (!dialogClosed.get()) {
                    try (DatagramSocket broadcastSocket = new DatagramSocket()) {
                        broadcastSocket.setBroadcast(true);
                        // 2 second timeout for responses
                        broadcastSocket.setSoTimeout(2000);

                        // Send a packet to 255.255.255.255 on port 8888
                        DatagramPacket probe = new DatagramPacket(packetData, packetData.length, InetAddress.getByName("255.255.255.255"), 8888);
                        broadcastSocket.send(probe);

                        // Broadcast the message over all the network interfaces
                        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
                        while (interfaces.hasMoreElements()) {
                            NetworkInterface networkInterface = interfaces.nextElement();
                            if (networkInterface.isLoopback() || !networkInterface.isUp()) continue;

                            for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
                                InetAddress broadcast = interfaceAddress.getBroadcast();
                                if (broadcast == null) continue;
                                try {
                                    broadcastSocket.send(new DatagramPacket(packetData, packetData.length, broadcast, 8888));
                                    logger.trace("Sent UDP Broadcast to {}", broadcast.getHostAddress());
                                } catch (IOException e) {
                                    logger.trace("Unable to broadcast to {}: {}", broadcast.getHostAddress(), e.getMessage());
                                }
                            }
                        }

                        try {
                            while (true) { // the socket timeout ends this
                                byte[] recvBuf = new byte[1500];
                                DatagramPacket receivePacket = new DatagramPacket(recvBuf, recvBuf.length);
                                broadcastSocket.receive(receivePacket);

                                String message = new String(receivePacket.getData(), 0, receivePacket.getLength(), StandardCharsets.UTF_8).trim();
                                String host = receivePacket.getAddress().getHostAddress();
                                logger.debug("PikaReader Discover Response: {} \"{}\"", host, message);

                                // PIKA_READER <unitID> <port>
                                String[] msg = message.split("\\s+");
                                if (msg.length < 3 || !msg[0].equalsIgnoreCase("PIKA_READER")) {
                                    logger.debug("Ignoring a malformed discovery response from {}", host);
                                    continue;
                                }
                                DiscoveredReader u = new DiscoveredReader(host, msg[1].toUpperCase(), msg[2], false);
                                Platform.runLater(() -> {
                                    if (!readers.contains(u)) readers.add(u);
                                });
                            }
                        } catch (IOException ex) {
                            // timeout, go around again
                        }
                    } catch (IOException ex) {
                        logger.debug("PikaReader discovery error: {}", ex.getMessage());
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return null;
                        }
                    }
                }
                logger.debug("Done scanning for PikaReader units.");
                return null;
            }
        };
        Thread scanner = new Thread(pikaSearch);
        scanner.setDaemon(true);
        scanner.setName("Pika Scanner");
        scanner.start();

        ProgressBar progress = new ProgressBar();
        progress.progressProperty().bind(pikaSearch.progressProperty());

        ListView<DiscoveredReader> pikaReaderListView = new ListView<>();
        pikaReaderListView.setItems(readers);

        Dialog<DiscoveredReader> dialog = new Dialog<>();
        dialog.resizableProperty().set(true);
        dialog.getDialogPane().setMaxHeight(Screen.getPrimary().getVisualBounds().getHeight() - 150);
        dialog.getDialogPane().getStylesheets().add(PikaReaderDirectReader.class.getResource("PikaReaderDirect.css").toExternalForm());
        dialog.setTitle("Select PikaReader");
        // Keep this short enough for one line: the dialog is sized to the manual entry row
        dialog.setHeaderText("Discover PikaReader units or enter an address");
        ButtonType selectButtonType = new ButtonType("Select", ButtonBar.ButtonData.OK_DONE);
        dialog.getDialogPane().getButtonTypes().addAll(selectButtonType, ButtonType.CANCEL);

        VBox mainVBox = new VBox();
        mainVBox.setSpacing(5);
        mainVBox.setStyle("-fx-font-size: 16px;"); // Make the scroll bar a bit larger

        VBox progressVBox = new VBox();
        progressVBox.setAlignment(Pos.CENTER);
        progressVBox.getChildren().add(new Label("Searching for Units..."));
        progressVBox.visibleProperty().bind(scanCompleted.not());
        progressVBox.managedProperty().bind(scanCompleted.not());
        progressVBox.getChildren().add(progress);
        Label foundCount = new Label();
        foundCount.textProperty().bind(Bindings.concat("Found ", Bindings.size(readers).asString()));
        progressVBox.getChildren().add(foundCount);
        progress.setMaxWidth(500);
        progressVBox.setPrefHeight(175);

        VBox listVBox = new VBox();
        listVBox.fillWidthProperty().set(true);
        listVBox.setAlignment(Pos.CENTER_LEFT);

        Label selectLabel = new Label("Select a PikaReader:");
        selectLabel.visibleProperty().bind(Bindings.size(readers).isNotEqualTo(0));
        selectLabel.managedProperty().bind(Bindings.size(readers).isNotEqualTo(0));
        listVBox.getChildren().add(selectLabel);
        listVBox.getChildren().add(pikaReaderListView);

        Label notFound = new Label("No PikaReader units were found!\nCheck network settings\nand try again.");
        notFound.visibleProperty().bind(Bindings.size(readers).isEqualTo(0));
        notFound.managedProperty().bind(Bindings.size(readers).isEqualTo(0));
        pikaReaderListView.visibleProperty().bind(Bindings.size(readers).greaterThanOrEqualTo(1));
        pikaReaderListView.managedProperty().bind(Bindings.size(readers).greaterThanOrEqualTo(1));

        listVBox.setPrefHeight(1750);
        // Shrink the list, not the manual entry, when the dialog is short
        VBox.setVgrow(listVBox, Priority.ALWAYS);
        listVBox.getChildren().add(notFound);
        listVBox.visibleProperty().bind(scanCompleted);
        listVBox.managedProperty().bind(scanCompleted);

        // Manual entry: protocol, IP address or hostname, and port (blank for 80 / 443)
        ChoiceBox<String> protocolChoiceBox = new ChoiceBox<>(FXCollections.observableArrayList(HTTP, HTTPS));
        protocolChoiceBox.setValue(HTTP);
        TextField hostTextField = new TextField();
        hostTextField.setPromptText("IP address or hostname");
        hostTextField.setStyle("-fx-pref-width: 10.75em;");
        HBox.setHgrow(hostTextField, Priority.ALWAYS);
        TextField portTextField = new TextField();
        portTextField.setStyle("-fx-pref-width: 4.5em;");
        portTextField.setTextFormatter(new TextFormatter<String>(change
                -> change.getControlNewText().matches("\\d{0,5}") ? change : null));
        portTextField.promptTextProperty().bind(Bindings.when(protocolChoiceBox.valueProperty().isEqualTo(HTTPS)).then("443").otherwise("80"));
        HBox manualHBox = new HBox(5, protocolChoiceBox, hostTextField, new Label(":"), portTextField);
        manualHBox.setAlignment(Pos.CENTER_LEFT);

        Label manualStatusLabel = new Label();
        manualStatusLabel.setWrapText(true);
        manualStatusLabel.setMinHeight(Region.USE_PREF_SIZE);

        VBox manualVBox = new VBox(5, new Separator(), new Label("Or enter a PikaReader's address:"), manualHBox, manualStatusLabel);
        manualVBox.setMinHeight(Region.USE_PREF_SIZE);

        mainVBox.getChildren().addAll(progressVBox, listVBox, manualVBox);
        dialog.getDialogPane().setContent(mainVBox);

        scanCompleted.bind(Bindings.size(readers).greaterThanOrEqualTo(1));

        // The manual reader, once GET /status on it works
        BooleanProperty manualOK = new SimpleBooleanProperty(false);
        ObjectProperty<DiscoveredReader> manualReader = new SimpleObjectProperty<>();
        // Bumped on every edit, so the results of older checks are ignored
        AtomicInteger manualCheck = new AtomicInteger();
        // Wait for the typing to stop before checking
        PauseTransition manualDebounce = new PauseTransition(javafx.util.Duration.millis(500));

        InvalidationListener manualEdited = o -> {
            manualCheck.incrementAndGet();
            manualOK.set(false);
            manualReader.set(null);
            manualDebounce.stop();

            String host = hostTextField.getText().trim();
            String port = portTextField.getText();
            if (host.isEmpty()) {
                setStatus(manualStatusLabel, "", null);
            } else if (!MANUAL_HOST_PATTERN.matcher(host).matches()) {
                setStatus(manualStatusLabel, "Enter just the IP address or hostname", "warning-label");
            } else if (!port.isEmpty() && (Integer.parseInt(port) < 1 || Integer.parseInt(port) > 65535)) {
                setStatus(manualStatusLabel, "The port must be between 1 and 65535", "warning-label");
            } else {
                setStatus(manualStatusLabel, "Checking...", null);
                manualDebounce.playFromStart();
            }
        };
        protocolChoiceBox.valueProperty().addListener(manualEdited);
        hostTextField.textProperty().addListener(manualEdited);
        portTextField.textProperty().addListener(manualEdited);

        manualDebounce.setOnFinished(event -> {
            int check = manualCheck.get();
            boolean secure = HTTPS.equals(protocolChoiceBox.getValue());
            String host = hostTextField.getText().trim();
            int portNumber = portTextField.getText().isEmpty() ? (secure ? 443 : 80) : Integer.parseInt(portTextField.getText());
            String port = String.valueOf(portNumber);

            probeStatus(secure, host, portNumber).whenComplete((unitID, ex) -> Platform.runLater(() -> {
                if (check != manualCheck.get() || dialogClosed.get()) return; // edited since, or closed
                if (ex != null) {
                    logger.debug("No PikaReader at {}{}:{}: {}", secure ? HTTPS : HTTP, host, port, errorMessage(ex));
                    setStatus(manualStatusLabel, errorMessage(ex), "warning-label");
                    return;
                }
                // Upper case, like discovery
                String name = unitID.toUpperCase();
                manualReader.set(new DiscoveredReader(host, name, port, secure));
                manualOK.set(true);
                setStatus(manualStatusLabel, "Found " + name, "ok-label");
            }));
        });

        // Pre-fill the reader this input is using now (this checks it too)
        if (configuredProperty.get()) {
            protocolChoiceBox.setValue(secureProperty.get() ? HTTPS : HTTP);
            portTextField.setText(readerPortProperty.getValueSafe());
            hostTextField.setText(readerIPProperty.getValueSafe());
        }

        // If they double click on a reader, select it and close the dialog box
        pikaReaderListView.setOnMouseClicked((MouseEvent click) -> {
            DiscoveredReader selected = pikaReaderListView.getSelectionModel().getSelectedItem();
            if (click.getClickCount() == 2 && selected != null) {
                dialog.setResult(selected);
            }
        });

        dialog.getDialogPane().getScene().getWindow().sizeToScene();

        // Select needs a discovered unit or a manual address that works
        Node selectButton = dialog.getDialogPane().lookupButton(selectButtonType);
        selectButton.disableProperty().bind(pikaReaderListView.getSelectionModel().selectedItemProperty().isNull().and(manualOK.not()));

        dialog.setResultConverter(dialogButton -> {
            if (dialogButton == selectButtonType) {
                DiscoveredReader selected = pikaReaderListView.getSelectionModel().getSelectedItem();
                if (selected != null) return selected;
                return manualOK.get() ? manualReader.get() : null;
            }
            return null;
        });

        Optional<DiscoveredReader> result = dialog.showAndWait();
        dialogClosed.set(true);
        manualDebounce.stop();

        result.ifPresent(r -> {
            logger.debug("Using PikaReader {} at {}", r.unitID(), baseURL(r.secure(), r.host(), r.port()));
            useReader(r.host(), r.port(), r.secure(), r.unitID());
        });
    }

    // ------------------------------------------------------------------
    // Properties for the controller
    // ------------------------------------------------------------------

    BooleanProperty readingProperty() { return readingProperty; }
    BooleanProperty connectedProperty() { return connectedProperty; }
    BooleanProperty enabledProperty() { return enabledProperty; }
    BooleanProperty configuredProperty() { return configuredProperty; }
    BooleanProperty secureProperty() { return secureProperty; }
    DoubleProperty batteryProperty() { return batteryProperty; }
    StringProperty rfidReaderProblemProperty() { return rfidReaderProblemProperty; }
    StringProperty readerNameProperty() { return readerNameProperty; }
    StringProperty readerIPProperty() { return readerIPProperty; }
    StringProperty readerPortProperty() { return readerPortProperty; }
    StringProperty lastReadProperty() { return lastReadProperty; }
    IntegerProperty lastUpdatedProperty() { return lastUpdatedProperty; }
    BooleanProperty profileSupportedProperty() { return profileSupportedProperty; }
    StringProperty profileIDProperty() { return profileIDProperty; }
    StringProperty profileNameProperty() { return profileNameProperty; }
    ObservableList<Profile> getProfiles() { return profiles; }

    // ------------------------------------------------------------------
    // Helper types
    // ------------------------------------------------------------------

    /**
     * A PikaReader reader profile (e.g. Finish Mats), as reported by the
     * PikaReader /profiles api and the profile object in the STATUS messages.
     */
    public record Profile(String id, String name, String description) {
        @Override
        public String toString() {
            return name; // Controls what is displayed in the ChoiceBox
        }
    }

    private record DiscoveredReader(String host, String unitID, String port, boolean secure) {
        @Override
        public String toString() {
            return unitID + " (" + host + ")";
        }
    }

    private record RewindRange(LocalDateTime from, LocalDateTime to) {}

    private class EventWebSocketClient extends WebSocketClient {
        private long messageCounter = 0;

        EventWebSocketClient(String ws) throws URISyntaxException {
            super(new URI(ws));
        }

        @Override
        public void onOpen(ServerHandshake handshakedata) {
            logger.debug("Connection to {} opened", readerNameProperty.getValueSafe());
        }

        @Override
        public void onClose(int code, String reason, boolean remote) {
            logger.warn("PikaReader {} closed with code {}: {}", readerNameProperty.getValueSafe(), code, reason);

            // A client we have already replaced (e.g. a different unit was selected)
            if (this != webSocketClient) return;

            Platform.runLater(() -> {
                if (this == webSocketClient) connectedProperty.set(false);
            });

            if (enabledProperty.get()) {
                // Reconnect in 5 seconds. The shared scheduler just waits,
                // the actual work is done on the FX thread (UI-bound properties).
                RECONNECT.schedule(() -> Platform.runLater(() -> {
                    if (!enabledProperty.get() || this != webSocketClient) return; // disconnected or replaced in the mean time
                    logger.debug("Attempting to reconnect to {}", readerNameProperty.getValueSafe());
                    connectedProperty.set(false);
                    connect();
                }), 5, TimeUnit.SECONDS);
            }
        }

        @Override
        public void onMessage(String message) {
            logger.trace("Received message #{}: {}", messageCounter++, message);
            processLine(message);
        }

        @Override
        public void onMessage(ByteBuffer message) {
            logger.debug("received ByteBuffer");
        }

        @Override
        public void onError(Exception ex) {
            logger.warn("Websocket error for {}: {}", readerNameProperty.getValueSafe(), ex.toString());
        }
    }
}
