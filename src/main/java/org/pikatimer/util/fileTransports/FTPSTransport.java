/*
 * Copyright (C) 2016 jcgarner
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import javafx.beans.property.StringProperty;
import javax.net.ssl.SSLException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ProtocolCommandEvent;
import org.apache.commons.net.ProtocolCommandListener;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.net.ftp.FTPSClient;
import org.pikatimer.results.ReportDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uploads files over explicit FTPS (AUTH TLS), falling back as gently as possible:
 * <ol>
 * <li>Encrypted data channel that resumes the control connection's TLS session
 * ({@link SessionReuseFTPSClient}).</li>
 * <li>The same, limited to TLS 1.2, if the server rejected the TLS 1.3 data
 * connection.</li>
 * <li>Unencrypted data channel (PROT C) if the server still rejects the
 * encrypted data connection. The login stays encrypted.</li>
 * <li>Plain FTP only if the server rejects AUTH TLS outright.</li>
 * </ol>
 * The fallbacks only ever move down this list, and only on a definite answer
 * from the server, never on a network error. They reset when the destination's
 * settings change.
 *
 * @author jcgarner
 */
public class FTPSTransport extends QueuedFileTransport {
    private static final Logger LOG = LoggerFactory.getLogger(FTPSTransport.class);

    private static final int CONNECT_TIMEOUT_MS = 10_000;
    private static final int CONTROL_TIMEOUT_MS = 30_000;
    private static final Duration DATA_TIMEOUT = Duration.ofSeconds(30);
    private static final int REPLY_DRAIN_TIMEOUT_MS = 5_000;
    private static final Pattern DATA_TLS_REPLY = Pattern.compile("(?i)reuse|resum|tls|ssl");
    private static final String TMP_SUFFIX = ".PikaTmp";

    /** How the data channel is protected. */
    enum DataProtection {
        PRIVATE("Connected FTPS", "encrypted"),
        PRIVATE_TLS12("Connected FTPS (TLS 1.2)", "encrypted, TLS 1.2 only"),
        CLEAR("Connected FTPS (data unencrypted)", "unencrypted (login still encrypted)");

        final String status;
        final String description;

        DataProtection(String status, String description) {
            this.status = status;
            this.description = description;
        }
    }

    /** The server answered AUTH TLS with a 5xx: it does not support FTPS. */
    private static class TlsNotSupportedException extends IOException {
        TlsNotSupportedException(String reply, Throwable cause) {
            super(reply, cause);
        }
    }

    /** The server refused PROT P. */
    private static class ProtPRejectedException extends IOException {
        ProtPRejectedException(String reply, Throwable cause) {
            super(reply, cause);
        }
    }

    /** The server refused the encrypted data connection, usually because the TLS session wasn't reused. */
    private static class DataTlsRejectedException extends IOException {
        final DataProtection next;

        DataTlsRejectedException(String reply, DataProtection next, Throwable cause) {
            super(reply, cause);
            this.next = next;
        }
    }

    private static final ProtocolCommandListener COMMAND_LOGGER = new ProtocolCommandListener() {
        @Override
        public void protocolCommandSent(ProtocolCommandEvent event) {
            String message = event.getMessage().trim();
            if (message.toUpperCase().startsWith("PASS")) message = "PASS **********";
            LOG.trace("FTP CMD SENT: {}", message);
        }

        @Override
        public void protocolReplyReceived(ProtocolCommandEvent event) {
            LOG.trace("FTP REPLY RCV: {}", event.getMessage().trim());
        }
    };

    // Worker thread only
    private FTPClient client;
    private boolean plainFtp = false;
    private DataProtection protection = DataProtection.PRIVATE;

    @Override
    protected String protocolName() {
        return "FTPS";
    }

    @Override
    protected int defaultPort() {
        return FTP.DEFAULT_PORT;
    }

    @Override
    protected void resetConnectionState() {
        plainFtp = false;
        protection = DataProtection.PRIVATE;
    }

    @Override
    protected boolean isConnected() {
        return client != null && client.isConnected();
    }

    @Override
    protected void connect(Endpoint endpoint) throws IOException {
        // Each fallback is one-way, so this loops at most twice
        while (true) {
            try {
                client = openClient(endpoint, plainFtp, protection, this::setStatus);
                logger.info("FTPS: connected to {} ({})", endpoint, describe(client, plainFtp, protection));
                return;
            } catch (TlsNotSupportedException e) {
                logger.warn("FTPS: {} rejected AUTH TLS ({}). Falling back to plain FTP, so the password and files are sent unencrypted.",
                        endpoint.host(), e.getMessage());
                plainFtp = true;
            } catch (ProtPRejectedException e) {
                if (protection == DataProtection.CLEAR) throw e;
                logger.warn("FTPS: {} rejected PROT P ({}). Using an unencrypted data channel; the login stays encrypted.",
                        endpoint.host(), e.getMessage());
                protection = DataProtection.CLEAR;
            }
        }
    }

    @Override
    protected void upload(String filename, String contents) throws IOException {
        String tmp = filename + TMP_SUFFIX;
        try {
            store(client, tmp, contents.getBytes(StandardCharsets.UTF_8), !plainFtp ? protection : null);
        } catch (DataTlsRejectedException e) {
            // Java uses each TLS 1.3 session ticket only once and only gets a couple per control
            // connection, so a server that requires session reuse typically rejects the third
            // upload. TLS 1.2 session IDs can be resumed any number of times.
            logger.warn("FTPS: {} rejected the {} data connection ({}). Switching to {} data connections.{}",
                    parent == null ? "server" : parent.getServer(), protection.description, e.getMessage(), e.next.description,
                    e.next == DataProtection.PRIVATE_TLS12 ? " (The server requires TLS session reuse, which works reliably with TLS 1.2.)" : "");
            protection = e.next;
            throw new RetryNowException("Data connection rejected: " + e.getMessage(), e);
        }
        // Upload to a temp file and rename it, so readers never see a partial file
        if (!client.rename(tmp, filename)) {
            String firstReply = reply(client);
            client.deleteFile(filename); // fails harmlessly if it isn't there
            if (!client.rename(tmp, filename)) {
                throw new IOException("Unable to rename " + tmp + " to " + filename + ": " + reply(client)
                        + " (first attempt: " + firstReply + ")");
            }
        }
    }

    @Override
    protected void keepAlive() throws IOException {
        if (!client.sendNoOp()) throw new IOException("NOOP failed: " + reply(client));
    }

    @Override
    protected void disconnectQuietly() {
        disconnectQuietly(client);
        client = null;
    }

    @Override
    protected String connectedStatus() {
        return plainFtp ? "Connected (insecure)" : protection.status;
    }

    @Override
    protected String transferringStatus(String filename) {
        if (plainFtp) return "Transferring (insecure) " + filename;
        if (protection == DataProtection.CLEAR) return "Transferring (clear) " + filename;
        return "Transferring (secure) " + filename;
    }

    /**
     * Connects, logs in, sets the data channel protection and changes to the
     * target directory. On failure the client is closed and an exception is thrown.
     */
    private static FTPClient openClient(Endpoint ep, boolean plain, DataProtection protection, Consumer<String> progress) throws IOException {
        FTPClient c;
        if (plain) {
            c = new FTPClient();
        } else {
            SessionReuseFTPSClient s = SessionReuseFTPSClient.create();
            if (protection == DataProtection.PRIVATE_TLS12) s.setEnabledProtocols(new String[]{"TLSv1.2"});
            c = s;
        }
        c.addProtocolCommandListener(COMMAND_LOGGER);
        c.setConnectTimeout(CONNECT_TIMEOUT_MS);
        c.setDefaultTimeout(CONTROL_TIMEOUT_MS); // read timeout on the control connection
        c.setDataTimeout(DATA_TIMEOUT);

        boolean ok = false;
        try {
            progress.accept(plain ? "Connecting (insecure)..." : "Connecting FTPS...");
            try {
                c.connect(ep.host(), ep.port());
            } catch (SSLException e) {
                // execAUTH throws SSLException(reply) for a non-234 reply. A failed
                // handshake also throws SSLException, but the last reply is then the 234.
                if (!plain && FTPReply.isNegativePermanent(c.getReplyCode())) {
                    throw new TlsNotSupportedException(reply(c), e);
                }
                throw e;
            }
            if (!FTPReply.isPositiveCompletion(c.getReplyCode())) {
                throw new IOException("Server refused the connection: " + reply(c));
            }

            progress.accept(plain ? "Logging in (insecure)..." : "Logging in...");
            if (!c.login(ep.username(), ep.password())) {
                throw new IOException("Login failed: " + reply(c));
            }

            if (c instanceof FTPSClient s) {
                s.execPBSZ(0);
                if (protection == DataProtection.CLEAR) {
                    s.execPROT("C");
                } else {
                    try {
                        s.execPROT("P");
                    } catch (SSLException e) {
                        throw new ProtPRejectedException(reply(c), e);
                    }
                }
            }
            c.enterLocalPassiveMode();
            if (!c.setFileType(FTP.BINARY_FILE_TYPE)) {
                throw new IOException("Unable to set binary mode: " + reply(c));
            }

            progress.accept("Changing Directories...");
            if (!c.changeWorkingDirectory(ep.basePath())) {
                String cwdReply = reply(c);
                if (!c.makeDirectory(ep.basePath())) {
                    throw new IOException("Unable to change to or create " + ep.basePath() + ": " + cwdReply + " / " + reply(c));
                }
                if (!c.changeWorkingDirectory(ep.basePath())) {
                    throw new IOException("Unable to change to " + ep.basePath() + ": " + reply(c));
                }
            }
            ok = true;
            return c;
        } finally {
            if (!ok) disconnectQuietly(c);
        }
    }

    /**
     * Stores one file. If protection is P (not null and not CLEAR) and the server
     * refuses the encrypted data connection, this throws DataTlsRejectedException
     * naming the next protection level to try.
     */
    private static void store(FTPClient c, String name, byte[] data, DataProtection protection) throws IOException {
        boolean classify = protection != null && protection != DataProtection.CLEAR;
        boolean stored;
        try (InputStream in = new ByteArrayInputStream(data)) {
            stored = c.storeFile(name, in);
        } catch (SSLException | SocketException e) {
            // The data connection broke. If the server says why, it is still waiting on the control connection.
            if (classify) {
                String pending = readPendingReply(c);
                if (pending != null && isDataTlsRejection(c.getReplyCode(), pending)) {
                    throw new DataTlsRejectedException(pending, nextProtection(c, protection), e);
                }
            }
            throw e;
        }
        if (!stored) {
            if (classify && isDataTlsRejection(c.getReplyCode(), c.getReplyString())) {
                throw new DataTlsRejectedException(reply(c), nextProtection(c, protection), null);
            }
            throw new IOException("Upload of " + name + " failed: " + reply(c));
        }
    }

    private static boolean isDataTlsRejection(int code, String reply) {
        if (code == 522) return true; // vsftpd: "522 SSL connection failed: session reuse required"
        boolean negative = FTPReply.isNegativeTransient(code) || FTPReply.isNegativePermanent(code);
        return negative && reply != null && DATA_TLS_REPLY.matcher(reply).find();
    }

    private static DataProtection nextProtection(FTPClient c, DataProtection current) {
        // Restricting to TLS 1.2 only helps if the control connection used something newer
        if (current == DataProtection.PRIVATE && c instanceof SessionReuseFTPSClient s
                && !"TLSv1.2".equals(s.getControlProtocol()) && s.getControlProtocol() != null) {
            return DataProtection.PRIVATE_TLS12;
        }
        return DataProtection.CLEAR;
    }

    /** Reads a reply the server sent after a failed data transfer. Returns null if none arrives. */
    private static String readPendingReply(FTPClient c) {
        try {
            c.setSoTimeout(REPLY_DRAIN_TIMEOUT_MS);
            c.getReply();
            return reply(c);
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }

    private static String describe(FTPClient c, boolean plain, DataProtection protection) {
        if (plain) return "plain FTP, unencrypted";
        String tls = c instanceof SessionReuseFTPSClient s ? s.getControlProtocol() : null;
        return (tls == null ? "TLS" : tls) + ", data channel " + protection.description;
    }

    private static String reply(FTPClient c) {
        return StringUtils.normalizeSpace(StringUtils.defaultString(c.getReplyString()));
    }

    private static void disconnectQuietly(FTPClient c) {
        if (c == null || !c.isConnected()) return;
        try {
            c.disconnect();
        } catch (IOException e) {
            LOG.trace("FTPS disconnect failed", e);
        }
    }

    @Override
    public void test(ReportDestination rd, StringProperty output) {
        Consumer<String> out = testOutput(output);
        startTestThread(() -> runTest(rd, out));
    }

    private void runTest(ReportDestination rd, Consumer<String> out) {
        boolean plain = false;
        DataProtection prot = DataProtection.PRIVATE;
        // Each fallback is one-way: at most plain FTP plus two data protection steps
        for (int attempt = 0; attempt < 4; attempt++) {
            FTPClient c = null;
            try {
                Endpoint ep = Endpoint.from(rd, defaultPort());
                out.accept("Connecting to " + ep.host() + ":" + ep.port() + (plain ? " via FTP..." : " via FTPS..."));
                c = openClient(ep, plain, prot, out);
                out.accept("Connected (" + describe(c, plain, prot) + ")");

                out.accept("Uploading a test file...");
                String testFile = "PikaTimer-test" + TMP_SUFFIX;
                store(c, testFile, "PikaTimer connection test\n".getBytes(StandardCharsets.UTF_8), plain ? null : prot);
                c.deleteFile(testFile);

                out.accept("");
                if (plain) out.accept("Warning: the server does not support FTPS. Files and the password are sent unencrypted.");
                else if (prot == DataProtection.CLEAR) out.accept("Warning: files are sent unencrypted (the login is encrypted).");
                out.accept("Success!");
                return;
            } catch (TlsNotSupportedException e) {
                out.accept("The server does not support FTPS (" + e.getMessage() + "), trying plain FTP...");
                plain = true;
            } catch (ProtPRejectedException e) {
                out.accept("The server refused an encrypted data channel (" + e.getMessage() + "), retrying without...");
                prot = DataProtection.CLEAR;
            } catch (DataTlsRejectedException e) {
                out.accept("The server rejected the encrypted data connection (" + e.getMessage() + ")");
                out.accept("Retrying with " + e.next.description + " data connections...");
                prot = e.next;
            } catch (IOException | RuntimeException e) {
                logger.debug("FTPS test failed", e);
                out.accept("Error: " + StringUtils.defaultIfBlank(e.getMessage(), e.toString()));
                out.accept("");
                out.accept("Test Failed!");
                return;
            } finally {
                disconnectQuietly(c);
            }
        }
        out.accept("Test Failed!");
    }
}
