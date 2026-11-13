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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import javafx.animation.Animation;
import javafx.animation.KeyFrame;
import javafx.animation.PauseTransition;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.beans.InvalidationListener;
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
import javafx.fxml.FXMLLoader;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.ButtonBar;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.DatePicker;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.control.TextFormatter;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import org.pikatimer.event.Event;
import org.pikatimer.timing.RawTimeData;
import org.pikatimer.timing.TimingListener;
import org.pikatimer.timing.TimingReader;
import org.pikatimer.timing.reader.TridentProtocol.TridentRead;
import org.pikatimer.timing.reader.TridentProtocol.TridentSummary;
import org.pikatimer.util.DurationFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pikatimer.timing.reader.ReaderUtil.errorMessage;
import static org.pikatimer.timing.reader.ReaderUtil.installTimeFilter;
import static org.pikatimer.timing.reader.ReaderUtil.runOnFX;
import static org.pikatimer.timing.reader.ReaderUtil.setStatus;
import static org.pikatimer.timing.reader.ReaderUtil.toDateTime;

/**
 * Connects a timing input to a Trident TimeMachine reader.
 *
 * Ports (all plain TCP, lines end in CR LF):
 * data (default 10001, the filtered port) -> "aa" tag reads, "ab" triggers,
 *     and the optional summary JSON (battery voltage, temperature)
 * command (default 9999) -> $GETDATE, $SETDATE:yyMMddHHmmss, $REPLAY [yyMMddHHmmss], $REPLAY stop
 *
 * Replayed reads come back on the data port, so they go through the same
 * path as the live reads and duplicates are dropped by the timing input.
 *
 * TODO: the public Trident documentation does not describe a command to
 * start or stop reading, so there is no Start / Stop Reader button. Add one
 * once the command is confirmed.
 *
 * @author John Garner
 */
public class TridentDirectReader implements TimingReader {
    private static final Logger logger = LoggerFactory.getLogger(TridentDirectReader.class);

    // Attribute keys (stored with the timing location input)
    private static final String ATTR_IP = "TridentDirect:ip";
    private static final String ATTR_DATA_PORT = "TridentDirect:data_port";
    private static final String ATTR_COMMAND_PORT = "TridentDirect:command_port";
    private static final String ATTR_NAME = "TridentDirect:name";
    private static final String ATTR_CONNECT = "TridentDirect:connect";

    // Seconds since we last heard from the unit, capped
    static final int LAST_UPDATED_MAX = 999;

    // Send a $GETDATE this often (seconds) to check the unit and its clock
    private static final int HEARTBEAT_SECONDS = 10;
    // Reconnect the data port after this many heartbeats fail in a row
    // (only once the command port has worked on this connection)
    private static final int HEARTBEAT_FAILURES = 3;
    // Consider a replay finished when no replayed reads arrive for this long
    private static final int REPLAY_IDLE_SECONDS = 30;

    private static final int CONNECT_TIMEOUT_MS = 5000;
    private static final int PROBE_TIMEOUT_MS = 3000;
    // Stop waiting for more reply lines after this much quiet
    private static final int REPLY_IDLE_MS = 3000;

    private static final DateTimeFormatter CLOCK_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // One shared thread for all of the reconnect attempts
    private static final ScheduledExecutorService RECONNECT = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        thread.setName("Trident-reconnect");
        return thread;
    });

    // Threads for the command port. Each reader runs its commands one at a time (commandLock).
    private static final ExecutorService COMMANDS = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        thread.setName("Trident-command");
        return thread;
    });

    private TimingListener timingListener;

    // Connection settings
    private final StringProperty readerIPProperty = new SimpleStringProperty("");
    private final IntegerProperty dataPortProperty = new SimpleIntegerProperty(TridentProtocol.DEFAULT_DATA_PORT);
    private final IntegerProperty commandPortProperty = new SimpleIntegerProperty(TridentProtocol.DEFAULT_COMMAND_PORT);
    private final BooleanProperty configuredProperty = new SimpleBooleanProperty(false);

    // Reader state (only updated on the FX thread)
    private final BooleanProperty connectedProperty = new SimpleBooleanProperty(false);
    private final BooleanProperty readingProperty = new SimpleBooleanProperty(false);
    private final BooleanProperty enabledProperty = new SimpleBooleanProperty(false);
    private final DoubleProperty batteryVoltageProperty = new SimpleDoubleProperty(-1);
    private final DoubleProperty temperatureProperty = new SimpleDoubleProperty(Double.NaN);
    private final StringProperty commandProblemProperty = new SimpleStringProperty("");
    private final ObjectProperty<Duration> clockOffsetProperty = new SimpleObjectProperty<>(null);
    private final StringProperty readerNameProperty = new SimpleStringProperty("");
    private final StringProperty lastReadProperty = new SimpleStringProperty("<No Reads>");
    private final IntegerProperty lastUpdatedProperty = new SimpleIntegerProperty(LAST_UPDATED_MAX);
    private final BooleanProperty replayActiveProperty = new SimpleBooleanProperty(false);
    private final IntegerProperty replayCountProperty = new SimpleIntegerProperty(-1);

    // Written by the data / command threads, pushed to the properties by the timeline
    private volatile LocalDateTime lastUpdated = null;
    private volatile String lastRead = null;
    private volatile double batteryVoltage = -1;
    private volatile double temperature = Double.NaN;

    // Replay (rewind) tracking. Replayed reads are the ones from before the replay was requested.
    private volatile boolean replayActive = false;
    private volatile LocalDateTime replayCutoff = null;     // unit clock
    private volatile LocalDateTime lastReplayRead = null;   // computer clock
    private final AtomicInteger replayCount = new AtomicInteger(-1);

    // Data connection. Bumping the generation retires the current connection
    // so that its thread does not reconnect or touch the state. socketLock
    // keeps a starting connection from slipping past a retire.
    private final AtomicInteger generation = new AtomicInteger();
    private final Object socketLock = new Object();
    private Socket dataSocket; // guarded by socketLock

    // Heartbeat
    private final AtomicBoolean heartbeatInFlight = new AtomicBoolean(false);
    private final AtomicInteger heartbeatFailures = new AtomicInteger();
    private volatile boolean heartbeatWorked = false;
    private int tick = 0;

    private final Object commandLock = new Object();
    private Timeline timeline;

    // UI
    private Parent controlRoot = null;
    private Pane displayPane = null;

    public TridentDirectReader() {
        readingProperty.bind(connectedProperty);
    }

    // ------------------------------------------------------------------
    // TimingReader
    // ------------------------------------------------------------------

    @Override
    public void setTimingListener(TimingListener t) {
        timingListener = t;

        String ip = Objects.toString(t.getAttribute(ATTR_IP), "").trim();
        int dataPort = parsePort(t.getAttribute(ATTR_DATA_PORT), TridentProtocol.DEFAULT_DATA_PORT);
        int commandPort = parsePort(t.getAttribute(ATTR_COMMAND_PORT), TridentProtocol.DEFAULT_COMMAND_PORT);
        String name = Objects.toString(t.getAttribute(ATTR_NAME), "").trim();
        boolean autoConnect = !"false".equalsIgnoreCase(t.getAttribute(ATTR_CONNECT));

        logger.debug("TridentDirect: settings for {}: ip={} data={} command={} name={} connect={}",
                t.getLocationName(), ip, dataPort, commandPort, name, autoConnect);

        runOnFX(() -> {
            setAddress(ip, dataPort, commandPort, name);
            if (configuredProperty.get() && autoConnect) enable();
        });
    }

    @Override
    public void showControls(Pane p) {
        if (controlRoot == null) {
            try {
                FXMLLoader loader = new FXMLLoader(TridentDirectReader.class.getResource("TridentDirectReader.fxml"));
                controlRoot = loader.load();
                TridentDirectReaderController controller = loader.getController();
                controller.setReader(this);
            } catch (IOException ex) {
                logger.error("Unable to load the Trident controls", ex);
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
            if (configuredProperty.get()) enable();
        });
    }

    /**
     * Called when the input is no longer needed (type change, removal).
     * This only disconnects from the unit and does not change the saved
     * auto connect setting.
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

    private static int parsePort(String value, int defaultPort) {
        try {
            int port = Integer.parseInt(Objects.toString(value, "").trim());
            return (port >= 1 && port <= 65535) ? port : defaultPort;
        } catch (NumberFormatException ex) {
            return defaultPort;
        }
    }

    // Must be called on the FX thread
    private void setAddress(String ip, int dataPort, int commandPort, String name) {
        readerIPProperty.set(ip);
        dataPortProperty.set(dataPort);
        commandPortProperty.set(commandPort);
        readerNameProperty.set(name.isBlank() ? "Trident " + ip : name);
        configuredProperty.set(!ip.isBlank());
    }

    // Must be called on the FX thread
    private void resetState() {
        batteryVoltage = -1;
        temperature = Double.NaN;
        batteryVoltageProperty.set(-1);
        temperatureProperty.set(Double.NaN);
        commandProblemProperty.set("");
        clockOffsetProperty.set(null);
        heartbeatFailures.set(0);
        heartbeatWorked = false;
        lastUpdated = null;
        lastUpdatedProperty.set(LAST_UPDATED_MAX);
        lastRead = null;
        lastReadProperty.set("<No Reads>");
        replayActive = false;
        replayActiveProperty.set(false);
        replayCount.set(-1);
        replayCountProperty.set(-1);
    }

    // Must be called on the FX thread
    void enable() {
        enabledProperty.set(true);
        startTimeline();
        connect();
    }

    // Must be called on the FX thread
    void disable() {
        enabledProperty.set(false);
        retireConnection();
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
    private void useReader(String ip, int dataPort, int commandPort, String name) {
        disable();
        resetState();
        setAddress(ip, dataPort, commandPort, name);
        setAttribute(ATTR_IP, ip);
        setAttribute(ATTR_DATA_PORT, String.valueOf(dataPort));
        setAttribute(ATTR_COMMAND_PORT, String.valueOf(commandPort));
        setAttribute(ATTR_NAME, name);
        setAttribute(ATTR_CONNECT, "true");
        enable();
    }

    private void setAttribute(String key, String value) {
        if (timingListener != null) timingListener.setAttribute(key, value);
    }

    // Must be called on the FX thread
    private void startTimeline() {
        if (timeline != null) timeline.stop();
        tick = 0;
        timeline = new Timeline(new KeyFrame(javafx.util.Duration.seconds(1), event -> onTick()));
        timeline.setCycleCount(Animation.INDEFINITE);
        timeline.play();
    }

    // Once a second on the FX thread: push the background updates to the
    // properties and send the heartbeat
    private void onTick() {
        String r = lastRead;
        if (r != null && !r.equals(lastReadProperty.get())) lastReadProperty.set(r);

        LocalDateTime updated = lastUpdated;
        long seconds = updated == null ? LAST_UPDATED_MAX : Duration.between(updated, LocalDateTime.now()).toSeconds();
        lastUpdatedProperty.set((int) Math.max(0, Math.min(seconds, LAST_UPDATED_MAX)));

        batteryVoltageProperty.set(batteryVoltage);
        temperatureProperty.set(temperature);

        // Replay progress, and end it once the replayed reads stop arriving
        replayCountProperty.set(replayCount.get());
        if (replayActive) {
            LocalDateTime last = lastReplayRead;
            if (last != null && Duration.between(last, LocalDateTime.now()).toSeconds() > REPLAY_IDLE_SECONDS) {
                replayActive = false;
            }
        }
        replayActiveProperty.set(replayActive);

        if (connectedProperty.get() && tick++ % HEARTBEAT_SECONDS == 0) heartbeat();
    }

    // Must be called on the FX thread
    private void connect() {
        if (!configuredProperty.get()) return;

        int gen = retireConnection();
        String host = readerIPProperty.getValueSafe();
        int port = dataPortProperty.get();
        String name = readerNameProperty.getValueSafe();

        Thread dataThread = new Thread(() -> runDataConnection(gen, host, port, name));
        dataThread.setName("Thread-Trident-" + host);
        dataThread.setDaemon(true);
        dataThread.start();
    }

    // The data connection thread: connect, read lines until the connection
    // drops or is retired, then schedule a reconnect if we are still enabled
    private void runDataConnection(int gen, String host, int port, String name) {
        Socket socket = new Socket();
        try {
            // Register the socket first so a retire closes it, even mid-connect
            synchronized (socketLock) {
                if (gen != generation.get()) return; // already retired
                dataSocket = socket;
            }
            logger.debug("Connecting to {} at {}:{}", name, host, port);
            socket.setKeepAlive(true);
            socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);

            Platform.runLater(() -> {
                if (gen != generation.get()) return;
                heartbeatFailures.set(0);
                heartbeatWorked = false;
                tick = 0; // heartbeat on the next tick
                connectedProperty.set(true);
            });
            logger.info("Connected to {} at {}:{}", name, host, port);

            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.ISO_8859_1));
            String line;
            while ((line = in.readLine()) != null) {
                if (gen != generation.get()) break;
                processLine(line);
            }
            if (gen == generation.get()) logger.warn("{} closed the connection", name);
        } catch (IOException ex) {
            if (gen == generation.get()) logger.warn("Trident connection error for {}: {}", name, errorMessage(ex));
        } finally {
            try {
                socket.close();
            } catch (IOException ex) {
                // ignore
            }
            synchronized (socketLock) {
                if (dataSocket == socket) dataSocket = null;
            }

            Platform.runLater(() -> {
                if (gen == generation.get()) connectedProperty.set(false);
            });

            // Reconnect in 5 seconds. The shared scheduler just waits,
            // the actual work is done on the FX thread (UI-bound properties).
            if (gen == generation.get() && enabledProperty.get()) {
                RECONNECT.schedule(() -> Platform.runLater(() -> {
                    if (!enabledProperty.get() || gen != generation.get()) return; // disconnected or replaced in the mean time
                    logger.debug("Attempting to reconnect to {}", name);
                    connect();
                }), 5, TimeUnit.SECONDS);
            }
        }
    }

    // Retire the current data connection (its thread stops without
    // reconnecting) and close its socket.
    // @return the new generation, for a connection that replaces it
    private int retireConnection() {
        synchronized (socketLock) {
            int gen = generation.incrementAndGet();
            closeQuietly(dataSocket);
            dataSocket = null;
            return gen;
        }
    }

    // Drop the data connection of this generation so that its thread
    // reconnects (the unit stopped answering)
    private void dropConnection(int gen) {
        synchronized (socketLock) {
            if (gen == generation.get()) closeQuietly(dataSocket);
        }
    }

    private static void closeQuietly(Socket socket) {
        if (socket == null) return;
        try {
            socket.close();
        } catch (IOException ex) {
            // ignore
        }
    }

    // ------------------------------------------------------------------
    // Messages from the unit
    // ------------------------------------------------------------------

    private void processLine(String line) {
        line = line.trim();
        if (line.isEmpty()) return;

        // Any line means the unit is alive
        lastUpdated = LocalDateTime.now();

        try {
            if (TridentProtocol.isTagLine(line)) {
                Optional<TridentRead> read = TridentProtocol.parseTagLine(line);
                if (read.isPresent()) processRead(read.get());
                else logger.debug("Skipping an unreadable tag line from {}: {}", readerNameProperty.getValueSafe(), line);
            } else if (TridentProtocol.isSummaryLine(line)) {
                TridentProtocol.parseSummary(line).ifPresent(this::processSummary);
            } else if (TridentProtocol.isTriggerLine(line)) {
                // TODO: could be used to set a wave start time
                logger.info("Trigger from {}: {}", readerNameProperty.getValueSafe(), line);
            } else {
                logger.debug("Unknown line from {}: {}", readerNameProperty.getValueSafe(), line);
            }
        } catch (RuntimeException ex) {
            logger.warn("Unable to process a line from {}: {}", readerNameProperty.getValueSafe(), line, ex);
        }
    }

    private void processSummary(TridentSummary summary) {
        logger.trace("Trident summary from {}: {}", readerNameProperty.getValueSafe(), summary);
        if (summary.batteryVoltage() > 0) batteryVoltage = summary.batteryVoltage();
        if (!Double.isNaN(summary.temperature())) temperature = summary.temperature();
    }

    /**
     * Hands a read to the timing listener (which ignores duplicates).
     */
    private void processRead(TridentRead read) {
        logger.trace("Chip read from {}: {}", readerNameProperty.getValueSafe(), read);

        TimingListener listener = timingListener;
        if (listener == null) return;

        // Count the replayed reads (the ones from before the replay was requested)
        LocalDateTime cutoff = replayCutoff;
        if (replayActive && cutoff != null && read.time().isBefore(cutoff)) {
            replayCount.incrementAndGet();
            lastReplayRead = LocalDateTime.now();
        }

        Duration time = Duration.between(Event.getInstance().getLocalEventDate().atStartOfDay(), read.time());
        if (time.isNegative()) {
            logger.debug("Read of {} at {} is before the event date, ignoring", read.chip(), read.time());
            return;
        }

        RawTimeData rawTime = new RawTimeData();
        rawTime.setChip(read.chip());
        rawTime.setTimestampLong(time.toNanos());
        listener.processRead(rawTime);

        lastRead = read.chip() + " @ " + DurationFormatter.durationToString(time, 3);
    }

    // ------------------------------------------------------------------
    // Commands
    // ------------------------------------------------------------------

    // Must be called on the FX thread (reads the address properties)
    CompletableFuture<List<String>> sendCommand(String command) {
        return sendCommand(() -> command, false);
    }

    // Must be called on the FX thread (reads the address properties)
    private CompletableFuture<List<String>> sendCommand(Supplier<String> command, boolean alignToSecond) {
        String host = readerIPProperty.getValueSafe();
        int port = commandPortProperty.get();
        String name = readerNameProperty.getValueSafe();
        return CompletableFuture.supplyAsync(() -> {
            synchronized (commandLock) {
                try {
                    List<String> reply = exchange(host, port, command, alignToSecond, CONNECT_TIMEOUT_MS);
                    logger.debug("{} replied {}", name, reply);
                    return reply;
                } catch (IOException ex) {
                    throw new CompletionException(ex);
                }
            }
        }, COMMANDS);
    }

    /**
     * Sends one command over a new connection to the command port and
     * collects the reply lines until a final reply (OK, DATE:, an error) or
     * until the unit goes quiet.
     *
     * TODO verify against a real unit that the command port takes a new
     * connection per command.
     *
     * @param alignToSecond wait for the next whole second (after connecting)
     * before building and sending the command, for setting the clock
     * @throws IOException if the unit can't be reached or rejects the command
     */
    private static List<String> exchange(String host, int port, Supplier<String> command,
            boolean alignToSecond, int timeoutMS) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), timeoutMS);
            socket.setSoTimeout(REPLY_IDLE_MS);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.ISO_8859_1));
            Writer out = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.ISO_8859_1);

            if (alignToSecond) {
                long nanos = LocalDateTime.now().getNano();
                try {
                    Thread.sleep(Duration.ofNanos(1_000_000_000L - nanos));
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted");
                }
            }

            String cmd = command.get();
            logger.debug("Sending {} to {}:{}", cmd, host, port);
            out.write(cmd + "\r\n");
            out.flush();

            List<String> reply = new ArrayList<>();
            try {
                String line;
                while ((line = in.readLine()) != null) {
                    // Drop any telnet negotiation or other control characters
                    line = line.replaceAll("[^\\x20-\\x7E]", "").trim();
                    if (line.isEmpty() || line.equalsIgnoreCase(cmd)) continue; // blank, or an echo
                    reply.add(line);
                    if (TridentProtocol.isFinalReply(line)) break;
                }
            } catch (SocketTimeoutException ex) {
                // the unit went quiet, that is the end of the reply
            }

            if (reply.stream().anyMatch(TridentProtocol::isErrorReply)) {
                throw new IOException("The reader rejected " + cmd + ": " + String.join(" ", reply));
            }
            return reply;
        }
    }

    private static Optional<LocalDateTime> findDate(List<String> reply) {
        return reply.stream()
                .map(TridentProtocol::parseGetDate)
                .flatMap(Optional::stream)
                .findFirst();
    }

    // The unit's clock minus the computer's clock. $GETDATE is in whole
    // seconds, so assume the middle of the unit's second, and compare it
    // to the middle of the request.
    private static Duration clockOffset(LocalDateTime unitTime, LocalDateTime sent, LocalDateTime received) {
        LocalDateTime computer = sent.plus(Duration.between(sent, received).dividedBy(2));
        return Duration.between(computer, unitTime.plusNanos(500_000_000));
    }

    // FX thread: check that the unit is there and how far off its clock is.
    private void heartbeat() {
        if (!heartbeatInFlight.compareAndSet(false, true)) return;

        int gen = generation.get();
        String name = readerNameProperty.getValueSafe();
        LocalDateTime sent = LocalDateTime.now();
        sendCommand(TridentProtocol.getDateCommand()).whenComplete((reply, ex) -> {
            LocalDateTime received = LocalDateTime.now();
            heartbeatInFlight.set(false);
            Optional<LocalDateTime> unitTime = ex == null ? findDate(reply) : Optional.empty();

            if (unitTime.isPresent()) {
                Duration offset = clockOffset(unitTime.get(), sent, received);
                heartbeatFailures.set(0);
                heartbeatWorked = true;
                lastUpdated = received;
                Platform.runLater(() -> {
                    if (gen != generation.get()) return;
                    clockOffsetProperty.set(offset);
                    commandProblemProperty.set("");
                });
                return;
            }

            int failures = heartbeatFailures.incrementAndGet();
            logger.debug("Heartbeat {} for {} failed: {}", failures, name,
                    ex != null ? errorMessage(ex) : "no DATE in reply " + reply);
            if (failures >= HEARTBEAT_FAILURES) {
                Platform.runLater(() -> {
                    if (gen != generation.get()) return;
                    commandProblemProperty.set("Command port not responding");
                    clockOffsetProperty.set(null);
                });
                // The unit stopped answering after it was working: the data
                // connection is probably dead too, so start over
                if (heartbeatWorked && gen == generation.get()) {
                    logger.warn("{} stopped responding, reconnecting", name);
                    heartbeatWorked = false;
                    dropConnection(gen);
                }
            }
        });
    }

    /**
     * Checks that a Trident answers at an address that was entered by hand.
     * The data port must accept a connection. The command port is checked
     * with $GETDATE, but the unit can still be used without it.
     */
    static CompletableFuture<ProbeResult> probe(String host, int dataPort, int commandPort) {
        return CompletableFuture.supplyAsync(() -> {
            String dataError = null;
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, dataPort), PROBE_TIMEOUT_MS);
            } catch (IOException | IllegalArgumentException ex) {
                dataError = errorMessage(ex);
            }

            String commandError = null;
            LocalDateTime unitTime = null;
            if (dataError == null) {
                try {
                    List<String> reply = exchange(host, commandPort, TridentProtocol::getDateCommand, false, PROBE_TIMEOUT_MS);
                    unitTime = findDate(reply).orElse(null);
                    if (unitTime == null) commandError = "No reply to $GETDATE";
                } catch (IOException | IllegalArgumentException ex) {
                    commandError = errorMessage(ex);
                }
            }
            return new ProbeResult(dataError, commandError, unitTime);
        }, COMMANDS);
    }

    record ProbeResult(String dataError, String commandError, LocalDateTime unitTime) {}

    // ------------------------------------------------------------------
    // Rewind (replay)
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

        Dialog<Optional<LocalDateTime>> dialog = new Dialog<>();
        dialog.setTitle("Rewind");
        dialog.setHeaderText("Replay the reads stored on " + readerNameProperty.getValueSafe());
        ButtonType rewindButtonType = new ButtonType("Rewind", ButtonBar.ButtonData.OK_DONE);
        dialog.getDialogPane().getButtonTypes().addAll(rewindButtonType, ButtonType.CANCEL);

        VBox rewindVBox = new VBox();
        rewindVBox.setSpacing(5.0);
        rewindVBox.setStyle("-fx-font-size: 16px;");

        HBox startHBox = new HBox();
        startHBox.setSpacing(5.0);
        startHBox.setAlignment(Pos.CENTER_LEFT);
        Label startLabel = new Label("From:");
        startLabel.setMinWidth(40);
        DatePicker startDate = new DatePicker();
        TextField startTime = new TextField();
        startHBox.getChildren().addAll(startLabel, startDate, startTime);

        CheckBox everything = new CheckBox("Replay everything stored on the reader");
        startHBox.disableProperty().bind(everything.selectedProperty());

        Label note = new Label("The reader replays everything from this time up to now.\nReads that were already received are ignored.");
        note.setStyle("-fx-font-size: 12px;");

        rewindVBox.getChildren().addAll(startHBox, everything, note);
        dialog.getDialogPane().setContent(rewindVBox);

        BooleanProperty startTimeOK = new SimpleBooleanProperty(false);
        installTimeFilter(startTime, startTimeOK);

        LocalDate eventDate = Event.getInstance().getLocalEventDate();
        startDate.setValue(eventDate);
        startTime.setText("00:00:00");

        Node rewindButton = dialog.getDialogPane().lookupButton(rewindButtonType);
        rewindButton.disableProperty().bind(everything.selectedProperty().not()
                .and(startTimeOK.not().or(startDate.valueProperty().isNull())));

        dialog.setResultConverter(dialogButton -> {
            if (dialogButton != rewindButtonType) return null;
            if (everything.isSelected()) return Optional.empty();
            return Optional.of(toDateTime(startDate.getValue(), startTime.getText()));
        });

        Optional<Optional<LocalDateTime>> result = dialog.showAndWait();
        if (result.isEmpty()) return;

        Optional<LocalDateTime> from = result.get();
        String command = from.map(TridentProtocol::replayCommand).orElse(TridentProtocol.replayAllCommand());
        String readerName = readerNameProperty.getValueSafe();
        logger.info("Rewind {}: {}", readerName, command);

        // Replayed reads are the ones before now, on the unit's clock
        Duration offset = clockOffsetProperty.get();
        replayCutoff = LocalDateTime.now().plus(offset == null ? Duration.ZERO : offset);
        lastReplayRead = LocalDateTime.now();
        replayCount.set(0);
        replayActive = true;
        replayActiveProperty.set(true);
        replayCountProperty.set(0);

        sendCommand(command).whenComplete((reply, ex) -> Platform.runLater(() -> {
            if (ex == null) return;
            replayActive = false;
            replayActiveProperty.set(false);
            logger.error("Rewind of {} failed: {}", readerName, errorMessage(ex));
            Alert alert = new Alert(AlertType.ERROR);
            alert.setTitle("Rewind");
            alert.setHeaderText("Unable to rewind " + readerName);
            alert.setContentText(errorMessage(ex));
            alert.show();
        }));
    }

    void stopReplay() {
        String readerName = readerNameProperty.getValueSafe();
        replayActive = false;
        replayActiveProperty.set(false);
        sendCommand(TridentProtocol.replayStopCommand()).whenComplete((reply, ex) -> {
            if (ex != null) logger.warn("Unable to stop the replay on {}: {}", readerName, errorMessage(ex));
        });
    }

    // ------------------------------------------------------------------
    // Clock
    // ------------------------------------------------------------------

    // Read the unit's clock, then offer to set it to this computer's time
    void syncClock() {
        String readerName = readerNameProperty.getValueSafe();
        LocalDateTime sent = LocalDateTime.now();
        sendCommand(TridentProtocol.getDateCommand()).whenComplete((reply, ex) -> {
            LocalDateTime received = LocalDateTime.now();
            Optional<LocalDateTime> unitTime = ex == null ? findDate(reply) : Optional.empty();
            Platform.runLater(() -> {
                if (unitTime.isEmpty()) {
                    Alert error = new Alert(AlertType.ERROR);
                    error.setTitle("Sync Clock");
                    error.setHeaderText("Unable to read the clock on " + readerName);
                    error.setContentText(ex != null ? errorMessage(ex) : "The reader did not answer $GETDATE");
                    error.showAndWait();
                    return;
                }
                Duration offset = clockOffset(unitTime.get(), sent, received);
                clockOffsetProperty.set(offset);
                confirmSetClock(readerName, unitTime.get(), offset);
            });
        });
    }

    private void confirmSetClock(String readerName, LocalDateTime unitTime, Duration offset) {
        Alert alert = new Alert(AlertType.CONFIRMATION);
        alert.setTitle("Sync Clock");
        alert.setHeaderText("Set the clock on " + readerName + " to this computer's time?");
        alert.setContentText("Reader:\t\t" + unitTime.format(CLOCK_FORMAT)
                + "\nThis computer:\t" + LocalDateTime.now().format(CLOCK_FORMAT)
                + "\nDifference:\t\t" + formatOffset(offset)
                + "\n\nOnly change the clock before the reads you care about start."
                + "\nReads already taken keep their old times.");
        Optional<ButtonType> result = alert.showAndWait();
        if (result.isEmpty() || result.get() != ButtonType.OK) return;

        logger.info("Setting the clock on {} (was off by {})", readerName, formatOffset(offset));
        sendCommand(() -> TridentProtocol.setDateCommand(LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS)), true)
                .whenComplete((reply, ex) -> Platform.runLater(() -> {
                    if (ex != null) {
                        logger.warn("Unable to set the clock on {}: {}", readerName, errorMessage(ex));
                        Alert error = new Alert(AlertType.ERROR);
                        error.setTitle("Sync Clock");
                        error.setHeaderText("Unable to set the clock on " + readerName);
                        error.setContentText(errorMessage(ex));
                        error.showAndWait();
                        return;
                    }
                    // Check it right away
                    heartbeat();
                }));
    }

    // e.g. "+2.4s" (reader ahead) or "-0.5s" (reader behind)
    static String formatOffset(Duration offset) {
        if (offset == null) return "unknown";
        double seconds = offset.toMillis() / 1000.0;
        return String.format("%+.1fs", seconds);
    }

    // ------------------------------------------------------------------
    // Reader selection
    // ------------------------------------------------------------------

    // Just a host name or IP address (no scheme, path, user or spaces)
    private static final Pattern MANUAL_HOST_PATTERN = Pattern.compile("[^\\s/@?#:]+|\\[?[0-9a-fA-F:]+\\]?");

    void selectReader() {
        Dialog<Selection> dialog = new Dialog<>();
        dialog.getDialogPane().getStylesheets().add(TridentDirectReader.class.getResource("PikaReaderDirect.css").toExternalForm());
        dialog.setTitle("Select Trident Reader");
        dialog.setHeaderText("Enter the Trident TimeMachine's address");
        ButtonType selectButtonType = new ButtonType("Select", ButtonBar.ButtonData.OK_DONE);
        dialog.getDialogPane().getButtonTypes().addAll(selectButtonType, ButtonType.CANCEL);

        TextField hostTextField = new TextField();
        hostTextField.setPromptText("192.168.0.101");
        TextField dataPortTextField = portField(TridentProtocol.DEFAULT_DATA_PORT);
        TextField commandPortTextField = portField(TridentProtocol.DEFAULT_COMMAND_PORT);
        TextField nameTextField = new TextField();
        nameTextField.setPromptText("Optional, e.g. Finish A");

        GridPane grid = new GridPane();
        grid.setHgap(5);
        grid.setVgap(5);
        grid.addRow(0, new Label("IP address or hostname:"), hostTextField);
        grid.addRow(1, new Label("Data port (filtered):"), dataPortTextField);
        grid.addRow(2, new Label("Command port:"), commandPortTextField);
        grid.addRow(3, new Label("Name:"), nameTextField);
        GridPane.setHgrow(hostTextField, Priority.ALWAYS);

        Label statusLabel = new Label();
        statusLabel.setWrapText(true);
        statusLabel.setMinHeight(Region.USE_PREF_SIZE);
        statusLabel.setMaxWidth(420);

        VBox mainVBox = new VBox(10, grid, statusLabel);
        mainVBox.setPadding(new Insets(5));
        dialog.getDialogPane().setContent(mainVBox);

        BooleanProperty addressOK = new SimpleBooleanProperty(false);
        // Bumped on every edit, so the results of older checks are ignored
        AtomicInteger check = new AtomicInteger();
        BooleanProperty dialogClosed = new SimpleBooleanProperty(false);
        // Wait for the typing to stop before checking
        PauseTransition debounce = new PauseTransition(javafx.util.Duration.millis(500));

        InvalidationListener edited = o -> {
            check.incrementAndGet();
            addressOK.set(false);
            debounce.stop();

            String host = hostTextField.getText().trim();
            if (host.isEmpty()) {
                setStatus(statusLabel, "", null);
            } else if (!MANUAL_HOST_PATTERN.matcher(host).matches()) {
                setStatus(statusLabel, "Enter just the IP address or hostname", "warning-label");
            } else if (!validPort(dataPortTextField.getText()) || !validPort(commandPortTextField.getText())) {
                setStatus(statusLabel, "The ports must be between 1 and 65535", "warning-label");
            } else {
                setStatus(statusLabel, "Checking...", null);
                debounce.playFromStart();
            }
        };
        hostTextField.textProperty().addListener(edited);
        dataPortTextField.textProperty().addListener(edited);
        commandPortTextField.textProperty().addListener(edited);

        debounce.setOnFinished(event -> {
            int thisCheck = check.get();
            String host = hostTextField.getText().trim();
            int dataPort = portValue(dataPortTextField, TridentProtocol.DEFAULT_DATA_PORT);
            int commandPort = portValue(commandPortTextField, TridentProtocol.DEFAULT_COMMAND_PORT);

            probe(host, dataPort, commandPort).whenComplete((probe, ex) -> Platform.runLater(() -> {
                if (thisCheck != check.get() || dialogClosed.get()) return; // edited since, or closed
                if (ex != null) {
                    setStatus(statusLabel, errorMessage(ex), "warning-label");
                } else if (probe.dataError() != null) {
                    setStatus(statusLabel, "Data port " + dataPort + ": " + probe.dataError(), "warning-label");
                } else if (probe.commandError() != null) {
                    // Live reads will work, rewind and the clock will not
                    addressOK.set(true);
                    setStatus(statusLabel, "Data port OK, but the command port " + commandPort
                            + " did not answer (" + probe.commandError() + ").\nLive reads will work; rewind and clock sync will not.", "warning-label");
                } else {
                    addressOK.set(true);
                    Duration offset = clockOffset(probe.unitTime(), LocalDateTime.now(), LocalDateTime.now());
                    setStatus(statusLabel, "Found a reader. Its clock reads " + probe.unitTime().format(CLOCK_FORMAT)
                            + " (" + formatOffset(offset) + ")", "ok-label");
                }
            }));
        });

        // Pre-fill the reader this input is using now (this checks it too)
        if (configuredProperty.get()) {
            dataPortTextField.setText(String.valueOf(dataPortProperty.get()));
            commandPortTextField.setText(String.valueOf(commandPortProperty.get()));
            String name = readerNameProperty.getValueSafe();
            if (!name.equals("Trident " + readerIPProperty.getValueSafe())) nameTextField.setText(name);
            hostTextField.setText(readerIPProperty.getValueSafe());
        }

        Node selectButton = dialog.getDialogPane().lookupButton(selectButtonType);
        selectButton.disableProperty().bind(addressOK.not());

        dialog.setResultConverter(dialogButton -> {
            if (dialogButton != selectButtonType || !addressOK.get()) return null;
            return new Selection(hostTextField.getText().trim(),
                    portValue(dataPortTextField, TridentProtocol.DEFAULT_DATA_PORT),
                    portValue(commandPortTextField, TridentProtocol.DEFAULT_COMMAND_PORT),
                    nameTextField.getText().trim());
        });

        Optional<Selection> result = dialog.showAndWait();
        dialogClosed.set(true);
        debounce.stop();

        result.ifPresent(s -> {
            logger.debug("Using Trident {} at {}:{} / {}", s.name(), s.host(), s.dataPort(), s.commandPort());
            useReader(s.host(), s.dataPort(), s.commandPort(), s.name());
        });
    }

    private static TextField portField(int defaultPort) {
        TextField field = new TextField();
        field.setPromptText(String.valueOf(defaultPort));
        field.setStyle("-fx-pref-width: 6em;");
        field.setTextFormatter(new TextFormatter<String>(change
                -> change.getControlNewText().matches("\\d{0,5}") ? change : null));
        return field;
    }

    // Blank (use the default) or 1-65535
    private static boolean validPort(String text) {
        if (text.isEmpty()) return true;
        int port = Integer.parseInt(text);
        return port >= 1 && port <= 65535;
    }

    private static int portValue(TextField field, int defaultPort) {
        return field.getText().isEmpty() ? defaultPort : Integer.parseInt(field.getText());
    }

    private record Selection(String host, int dataPort, int commandPort, String name) {}

    // ------------------------------------------------------------------
    // Properties for the controller
    // ------------------------------------------------------------------

    String getAddress() {
        return readerIPProperty.getValueSafe() + " : " + dataPortProperty.get() + " / " + commandPortProperty.get();
    }

    BooleanProperty connectedProperty() { return connectedProperty; }
    BooleanProperty enabledProperty() { return enabledProperty; }
    BooleanProperty configuredProperty() { return configuredProperty; }
    DoubleProperty batteryVoltageProperty() { return batteryVoltageProperty; }
    DoubleProperty temperatureProperty() { return temperatureProperty; }
    StringProperty commandProblemProperty() { return commandProblemProperty; }
    ObjectProperty<Duration> clockOffsetProperty() { return clockOffsetProperty; }
    StringProperty readerNameProperty() { return readerNameProperty; }
    StringProperty readerIPProperty() { return readerIPProperty; }
    IntegerProperty dataPortProperty() { return dataPortProperty; }
    IntegerProperty commandPortProperty() { return commandPortProperty; }
    StringProperty lastReadProperty() { return lastReadProperty; }
    IntegerProperty lastUpdatedProperty() { return lastUpdatedProperty; }
    BooleanProperty replayActiveProperty() { return replayActiveProperty; }
    IntegerProperty replayCountProperty() { return replayCountProperty; }
}
