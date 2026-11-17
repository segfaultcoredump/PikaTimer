/*
 * Copyright (C) 2026 John Garner <segfaultcoredump@gmail.com>
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
package org.pikatimer.util.fileTransports;

import java.io.IOException;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import org.apache.commons.lang3.StringUtils;
import org.pikatimer.results.ReportDestination;
import org.pikatimer.util.DurationFormatter;
import org.pikatimer.util.FileTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for the remote transports (FTPS, SFTP). It owns the per-destination
 * upload queue and a single worker thread that connects, uploads, keeps the
 * connection alive while busy and backs off when something fails.
 *
 * Rules the worker follows:
 * <ul>
 * <li>Every failure path waits (5s, 10s, 20s, 40s, then 60s) before the next
 * attempt, so a broken server can never cause a tight reconnect loop. The only
 * exception is {@link RetryNowException}, which a subclass throws after it has
 * changed its connection strategy (a bounded number of times).</li>
 * <li>All network I/O happens on the worker thread. {@link #refreshConfig()}
 * only flags the change.</li>
 * <li>Failures are logged at WARN with the server's reason, so they reach the
 * log file.</li>
 * </ul>
 *
 * @author John Garner
 */
public abstract class QueuedFileTransport implements FileTransport {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private static final long[] BACKOFF_SECONDS = {5, 10, 20, 40, 60};
    private static final long POLL_SECONDS = 15;
    private static final long IDLE_DISCONNECT_NANOS = TimeUnit.SECONDS.toNanos(120);

    // Pending uploads. The map holds the newest contents for each file name and
    // the deque holds the names waiting to go out.
    private final LinkedBlockingDeque<String> transferQueue = new LinkedBlockingDeque<>();
    private final Map<String, String> transferMap = new ConcurrentHashMap<>();

    private final StringProperty transferStatus = new SimpleStringProperty("Idle");
    private final Object wakeLock = new Object();

    protected volatile ReportDestination parent;
    private volatile boolean needConfigRefresh = true;
    private volatile boolean running = true;
    private Thread worker; // guarded by this

    // Worker thread only
    private int consecutiveFailures = 0;
    private long lastActivityNanos = System.nanoTime();

    /** Short protocol name for logs and thread names, e.g. "FTPS". */
    protected abstract String protocolName();

    /** The port used when the server field has no explicit ":port". */
    protected abstract int defaultPort();

    /** Forget any per-server strategy (TLS fallbacks etc.). Called after a config change. */
    protected abstract void resetConnectionState();

    protected abstract boolean isConnected();

    /** Connect, log in and change to the target directory, or throw. */
    protected abstract void connect(Endpoint endpoint) throws Exception;

    protected abstract void upload(String filename, String contents) throws Exception;

    /** Called every 15s while connected and idle. Throw if the connection is dead. */
    protected abstract void keepAlive() throws Exception;

    /** Close the connection, ignoring errors. */
    protected abstract void disconnectQuietly();

    protected abstract String connectedStatus();

    protected abstract String transferringStatus(String filename);

    /**
     * Thrown by a subclass after it switched to a different connection strategy
     * (for example a TLS fallback). The file is retried immediately with no backoff.
     */
    protected static class RetryNowException extends IOException {
        public RetryNowException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** Where to connect. toString() deliberately leaves out the password. */
    protected record Endpoint(String host, int port, String username, String password, String basePath) {

        static Endpoint from(ReportDestination rd, int defaultPort) throws IOException {
            if (rd == null) throw new IOException("No destination configured");
            String server = StringUtils.trimToEmpty(rd.getServer());
            if (server.isEmpty()) throw new IOException("No server configured");
            String host = server;
            int port = defaultPort;
            int colon = server.indexOf(':');
            // host:port, but leave bare IPv6 addresses (more than one colon) alone
            if (colon > 0 && colon == server.lastIndexOf(':')) {
                host = server.substring(0, colon);
                try {
                    port = Integer.parseInt(server.substring(colon + 1).trim());
                } catch (NumberFormatException e) {
                    throw new IOException("Invalid port in server address \"" + server + "\"");
                }
            }
            return new Endpoint(host, port,
                    StringUtils.defaultString(rd.getUsername()),
                    StringUtils.defaultString(rd.getPassword()),
                    StringUtils.defaultString(rd.getBasePath()));
        }

        @Override
        public String toString() {
            return username + "@" + host + ":" + port;
        }
    }

    @Override
    public void save(String filename, String contents) {
        logger.debug("{}Transport.save() called for {}", protocolName(), filename);
        ReportDestination rd = parent;
        if (rd != null && Boolean.TRUE.equals(rd.getStripAccents())) contents = StringUtils.stripAccents(contents);
        transferMap.put(filename, contents);
        if (!transferQueue.contains(filename)) transferQueue.add(filename);
        ensureWorker();
    }

    @Override
    public void setOutputPortal(ReportDestination op) {
        parent = op;
        refreshConfig();
    }

    @Override
    public void refreshConfig() {
        // The worker applies this before its next step. Don't touch the
        // connection here, because this runs on the FX thread.
        needConfigRefresh = true;
        wakeWorker();
    }

    @Override
    public boolean isOK() {
        ReportDestination rd = parent;
        return rd != null && StringUtils.isNoneBlank(rd.getServer(), rd.getUsername(), rd.getPassword(), rd.getBasePath());
    }

    @Override
    public StringProperty statusProperty() {
        return transferStatus;
    }

    @Override
    public synchronized void shutdown() {
        running = false;
        wakeWorker();
        if (worker != null) worker.interrupt();
    }

    protected void setStatus(String status) {
        Platform.runLater(() -> transferStatus.set(status));
    }

    /** Appends lines to a test dialog's output. */
    protected static Consumer<String> testOutput(StringProperty output) {
        return line -> Platform.runLater(() -> {
            String current = output.getValueSafe();
            output.set(current.isEmpty() ? line : current + "\n" + line);
        });
    }

    /** Starts the worker for a test dialog in its own short-lived thread. */
    protected void startTestThread(Runnable test) {
        Thread t = new Thread(test, protocolName() + "-Transfer-Test");
        t.setDaemon(true);
        t.setUncaughtExceptionHandler((th, e) -> logger.error("{} test thread died", protocolName(), e));
        t.start();
    }

    private synchronized void ensureWorker() {
        if (!running || (worker != null && worker.isAlive())) return;
        ReportDestination rd = parent;
        String name = protocolName() + "-Transfer-" + (rd == null ? "?" : StringUtils.defaultString(rd.getServer()));
        worker = new Thread(this::runWorker, name);
        worker.setDaemon(true);
        worker.setUncaughtExceptionHandler((t, e) -> logger.error("{} transfer thread died unexpectedly", protocolName(), e));
        worker.start();
    }

    private void wakeWorker() {
        synchronized (wakeLock) {
            wakeLock.notifyAll();
        }
    }

    private void runWorker() {
        logger.debug("{} transfer thread started", protocolName());
        while (running) {
            String filename = null;
            try {
                if (needConfigRefresh) applyConfigRefresh();

                filename = transferQueue.poll(POLL_SECONDS, TimeUnit.SECONDS);
                if (filename == null) {
                    idle();
                    continue;
                }

                String contents = transferMap.get(filename);
                if (contents == null) {
                    // A duplicate entry for a file that has already been sent
                    logger.debug("{}: nothing pending for {}, skipping", protocolName(), filename);
                    filename = null;
                    continue;
                }

                if (!isConnected()) {
                    Endpoint endpoint = Endpoint.from(parent, defaultPort());
                    setStatus("Connecting...");
                    logger.debug("{}: connecting to {}", protocolName(), endpoint);
                    connect(endpoint);
                    lastActivityNanos = System.nanoTime();
                }

                setStatus(transferringStatus(filename));
                long start = System.nanoTime();
                upload(filename, contents);
                lastActivityNanos = System.nanoTime();

                // Only drop it if nobody saved a newer version while we were uploading
                transferMap.remove(filename, contents);
                if (consecutiveFailures > 0) logger.info("{}: uploads are working again", protocolName());
                consecutiveFailures = 0;
                logger.debug("{}: transfer of {} done in {}", protocolName(), filename,
                        DurationFormatter.durationToString(Duration.ofNanos(lastActivityNanos - start), 3, false, RoundingMode.HALF_EVEN));
                filename = null;
                setStatus(connectedStatus());
            } catch (InterruptedException e) {
                requeue(filename);
                if (!running) break;
            } catch (RetryNowException e) {
                // The subclass has already logged the reason and changed strategy
                requeue(filename);
                disconnectQuietly();
            } catch (Exception e) {
                requeue(filename);
                disconnectQuietly();
                failed(filename, e);
            }
        }
        disconnectQuietly();
        logger.debug("{} transfer thread stopped", protocolName());
    }

    private void applyConfigRefresh() {
        needConfigRefresh = false;
        logger.debug("{}: applying configuration change", protocolName());
        disconnectQuietly();
        resetConnectionState();
        consecutiveFailures = 0;
    }

    private void idle() {
        if (!isConnected()) return;
        if (System.nanoTime() - lastActivityNanos > IDLE_DISCONNECT_NANOS) {
            logger.debug("{}: idle for 2 minutes, disconnecting", protocolName());
            disconnectQuietly();
            setStatus("Idle");
            return;
        }
        try {
            keepAlive();
            setStatus(connectedStatus());
        } catch (Exception e) {
            // Servers drop idle connections all the time. Reconnect when the next file arrives.
            logger.debug("{}: keep-alive failed ({}), disconnecting", protocolName(), e.toString());
            disconnectQuietly();
            setStatus("Idle");
        }
    }

    private void requeue(String filename) {
        if (filename != null && transferMap.containsKey(filename) && !transferQueue.contains(filename)) {
            transferQueue.add(filename);
        }
    }

    private void failed(String filename, Exception e) {
        long delay = BACKOFF_SECONDS[Math.min(consecutiveFailures, BACKOFF_SECONDS.length - 1)];
        consecutiveFailures++;

        String target = parent == null ? "?" : parent.getServer();
        String what = filename == null ? "connection" : "upload of " + filename;
        String reason = StringUtils.defaultIfBlank(e.getMessage(), e.getClass().getSimpleName());
        if (consecutiveFailures == 1 || e instanceof RuntimeException) {
            logger.warn("{} {} to {} failed: {} (retrying in {}s)", protocolName(), what, target, e, delay, e);
        } else {
            logger.warn("{} {} to {} failed again (attempt {}): {} (retrying in {}s)",
                    protocolName(), what, target, consecutiveFailures, e, delay);
        }
        setStatus("Error: " + reason + " - retrying in " + delay + "s");
        pause(TimeUnit.SECONDS.toMillis(delay));
    }

    /** Waits for the backoff delay. A config change or shutdown ends the wait early. */
    private void pause(long millis) {
        long deadline = System.currentTimeMillis() + millis;
        synchronized (wakeLock) {
            long remaining;
            while (running && !needConfigRefresh && (remaining = deadline - System.currentTimeMillis()) > 0) {
                try {
                    wakeLock.wait(remaining);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }
}
