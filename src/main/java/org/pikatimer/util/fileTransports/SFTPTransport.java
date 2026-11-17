/*
 * Copyright (C) 2017 jcgarner
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

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.Slf4jLogger;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import javafx.beans.property.StringProperty;
import org.apache.commons.lang3.StringUtils;
import org.pikatimer.results.ReportDestination;

/**
 * Uploads files over SFTP (password authentication).
 *
 * @author jcgarner
 */
public class SFTPTransport extends QueuedFileTransport {

    static {
        // Send JSch's own connection, kex and auth messages to the PikaTimer log
        JSch.setLogger(new Slf4jLogger());
    }

    private static final int CONNECT_TIMEOUT_MS = 10_000;
    // JSch sends a keep-alive this often and drops the session after
    // SERVER_ALIVE_COUNT_MAX unanswered ones, so a dead server fails a put
    // instead of hanging it
    private static final int SERVER_ALIVE_INTERVAL_MS = 15_000;
    private static final int SERVER_ALIVE_COUNT_MAX = 3;

    private record Connection(Session session, ChannelSftp channel) {
        boolean isConnected() {
            return session.isConnected() && channel.isConnected() && !channel.isClosed();
        }

        void close() {
            channel.disconnect();
            session.disconnect();
        }
    }

    // Worker thread only
    private Connection connection;

    @Override
    protected String protocolName() {
        return "SFTP";
    }

    @Override
    protected int defaultPort() {
        return 22;
    }

    @Override
    protected void resetConnectionState() {
        // No per-server fallbacks for SFTP
    }

    @Override
    protected boolean isConnected() {
        return connection != null && connection.isConnected();
    }

    @Override
    protected void connect(Endpoint endpoint) throws IOException {
        connection = open(endpoint, this::setStatus);
        logger.info("SFTP: connected to {}", endpoint);
    }

    @Override
    protected void upload(String filename, String contents) throws IOException {
        try (InputStream in = new ByteArrayInputStream(contents.getBytes(StandardCharsets.UTF_8))) {
            connection.channel().put(in, filename, ChannelSftp.OVERWRITE); // synchronous
        } catch (SftpException e) {
            throw new IOException("Upload of " + filename + " failed: " + e.getMessage(), e);
        }
    }

    @Override
    protected void keepAlive() throws IOException {
        // JSch sends its own keep-alives (setServerAliveInterval); just notice a dropped session
        if (!isConnected()) throw new IOException("SFTP session closed");
    }

    @Override
    protected void disconnectQuietly() {
        if (connection != null) {
            try {
                connection.close();
            } catch (RuntimeException e) {
                logger.trace("SFTP disconnect failed", e);
            }
            connection = null;
        }
    }

    @Override
    protected String connectedStatus() {
        return "Connected";
    }

    @Override
    protected String transferringStatus(String filename) {
        return "Transferring " + filename;
    }

    /** Opens a session and SFTP channel and changes to the target directory, or throws. */
    private static Connection open(Endpoint ep, Consumer<String> progress) throws IOException {
        Session session = null;
        ChannelSftp channel = null;
        boolean ok = false;
        try {
            progress.accept("Connecting SFTP...");
            session = new JSch().getSession(ep.username(), ep.host(), ep.port());
            session.setPassword(ep.password().getBytes(StandardCharsets.UTF_8)); // only password authentication for now
            session.setConfig("StrictHostKeyChecking", "no");
            session.setTimeout(CONNECT_TIMEOUT_MS);
            session.setServerAliveInterval(SERVER_ALIVE_INTERVAL_MS);
            session.setServerAliveCountMax(SERVER_ALIVE_COUNT_MAX);

            progress.accept("Logging in...");
            session.connect(CONNECT_TIMEOUT_MS);

            progress.accept("Opening SFTP channel...");
            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect(CONNECT_TIMEOUT_MS);

            progress.accept("Changing Directories...");
            try {
                channel.cd(ep.basePath());
            } catch (SftpException e) {
                progress.accept("Target directory does not exist, attempting to create it...");
                try {
                    channel.mkdir(ep.basePath());
                    channel.cd(ep.basePath());
                } catch (SftpException e2) {
                    throw new IOException("Unable to change to or create " + ep.basePath() + ": " + e2.getMessage(), e2);
                }
            }
            ok = true;
            return new Connection(session, channel);
        } catch (JSchException e) {
            throw new IOException(e.getMessage(), e);
        } finally {
            if (!ok) {
                if (channel != null) channel.disconnect();
                if (session != null) session.disconnect();
            }
        }
    }

    @Override
    public void test(ReportDestination rd, StringProperty output) {
        Consumer<String> out = testOutput(output);
        startTestThread(() -> {
            Connection c = null;
            try {
                Endpoint ep = Endpoint.from(rd, defaultPort());
                out.accept("Connecting to " + ep.host() + ":" + ep.port() + "...");
                c = open(ep, out);
                out.accept("Connected");

                out.accept("Uploading a test file...");
                String testFile = "PikaTimer-test.PikaTmp";
                try (InputStream in = new ByteArrayInputStream("PikaTimer connection test\n".getBytes(StandardCharsets.UTF_8))) {
                    c.channel().put(in, testFile, ChannelSftp.OVERWRITE);
                    c.channel().rm(testFile);
                } catch (SftpException e) {
                    throw new IOException("Unable to upload a test file: " + e.getMessage(), e);
                }
                out.accept("");
                out.accept("Success!");
            } catch (IOException | RuntimeException e) {
                logger.debug("SFTP test failed", e);
                out.accept("Error: " + StringUtils.defaultIfBlank(e.getMessage(), e.toString()));
                out.accept("");
                out.accept("Test Failed!");
            } finally {
                if (c != null) c.close();
            }
        });
    }
}
