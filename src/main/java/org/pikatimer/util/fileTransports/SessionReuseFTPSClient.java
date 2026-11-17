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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketOption;
import java.nio.channels.SocketChannel;
import java.util.Set;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import org.apache.commons.net.ftp.FTPSClient;
import org.apache.commons.net.util.SSLContextUtils;
import org.apache.commons.net.util.TrustManagerUtils;

/**
 * Explicit FTPS (AUTH TLS) client whose protected data connections resume the
 * control connection's TLS session.
 *
 * Many servers (vsftpd with require_ssl_reuse, FileZilla Server, ProFTPD) only
 * accept a data connection that resumes the control connection's TLS session.
 * JSSE looks up a session to resume by the peer host and port it was given. Stock
 * FTPSClient connects its data SSLSocket straight to the passive port, so it never
 * finds the control session and always does a full handshake.
 *
 * This client keeps the data socket plain until it is connected, then layers TLS
 * over it. JSSE's client session cache is keyed by the peer host and the
 * underlying socket's remote port (SSLSocketImpl.getPeerPort() returns
 * getPort()), so the plain socket is wrapped in a {@link ControlPortSocket}. The
 * wrapper delegates everything to the real data socket but reports the control
 * connection's port. JSSE then offers the cached control session (a TLS 1.2 session
 * ID or ticket, or a TLS 1.3 PSK), which is the reuse those servers check for. This
 * uses only public APIs, with no reflection or --add-opens.
 *
 * @author John Garner
 */
public class SessionReuseFTPSClient extends FTPSClient {

    private final SSLContext sslContext;
    private boolean dataProtected = false;

    /**
     * Creates a client that trusts the server certificate the same way the
     * commons-net default does (it only checks that the certificate is valid
     * for the current date).
     */
    public static SessionReuseFTPSClient create() throws IOException {
        return new SessionReuseFTPSClient(SSLContextUtils.createSSLContext("TLS", null,
                TrustManagerUtils.getValidateServerCertificateTrustManager()));
    }

    private SessionReuseFTPSClient(SSLContext context) {
        super(false, context);
        this.sslContext = context;
    }

    @Override
    public void execPROT(String prot) throws SSLException, IOException {
        super.execPROT(prot);
        dataProtected = "P".equals(prot);
        if (dataProtected) {
            // super.execPROT installed an SSL socket factory for data connections.
            // Swap in plain sockets; _openDataConnection_ adds TLS itself.
            setSocketFactory(null);
            setServerSocketFactory(null);
        }
    }

    @Override
    protected Socket _openDataConnection_(String command, String arg) throws IOException {
        Socket socket = super._openDataConnection_(command, arg);
        if (socket == null || !dataProtected || socket instanceof SSLSocket) return socket;

        // Present the control connection's host and port so that JSSE resumes the control session
        int controlPort = getRemotePort();
        SSLSocket ssl = (SSLSocket) sslContext.getSocketFactory()
                .createSocket(new ControlPortSocket(socket, controlPort), _hostname_, controlPort, true);
        try {
            ssl.setUseClientMode(isClientMode());
            String[] protocols = getProtocols();
            if (protocols != null) ssl.setEnabledProtocols(protocols);
            String[] suites = getSuites();
            if (suites != null) ssl.setEnabledCipherSuites(suites);
            ssl.startHandshake();
        } catch (IOException | RuntimeException e) {
            try {
                ssl.close();
            } catch (IOException ignored) {
                // already failing
            }
            throw e;
        }
        return ssl;
    }

    /** Returns the TLS version of the control connection, e.g. "TLSv1.3", or null if not connected. */
    public String getControlProtocol() {
        return (_socket_ instanceof SSLSocket s) ? s.getSession().getProtocol() : null;
    }

    @Override
    public void disconnect() throws IOException {
        dataProtected = false;
        super.disconnect();
    }

    /**
     * A connected socket that forwards everything to the real data socket, except
     * that getPort() returns the control connection's port. JSSE only uses the port
     * for its session cache key, while all I/O goes through the delegate.
     */
    private static final class ControlPortSocket extends Socket {
        private final Socket delegate;
        private final int reportedPort;

        ControlPortSocket(Socket delegate, int reportedPort) {
            this.delegate = delegate;
            this.reportedPort = reportedPort;
        }

        @Override public int getPort() { return reportedPort; }

        @Override public InetAddress getInetAddress() { return delegate.getInetAddress(); }
        @Override public InetAddress getLocalAddress() { return delegate.getLocalAddress(); }
        @Override public int getLocalPort() { return delegate.getLocalPort(); }
        @Override public SocketAddress getRemoteSocketAddress() { return delegate.getRemoteSocketAddress(); }
        @Override public SocketAddress getLocalSocketAddress() { return delegate.getLocalSocketAddress(); }
        @Override public SocketChannel getChannel() { return delegate.getChannel(); }
        @Override public InputStream getInputStream() throws IOException { return delegate.getInputStream(); }
        @Override public OutputStream getOutputStream() throws IOException { return delegate.getOutputStream(); }
        @Override public void setTcpNoDelay(boolean on) throws SocketException { delegate.setTcpNoDelay(on); }
        @Override public boolean getTcpNoDelay() throws SocketException { return delegate.getTcpNoDelay(); }
        @Override public void setSoLinger(boolean on, int linger) throws SocketException { delegate.setSoLinger(on, linger); }
        @Override public int getSoLinger() throws SocketException { return delegate.getSoLinger(); }
        @Override public void sendUrgentData(int data) throws IOException { delegate.sendUrgentData(data); }
        @Override public void setOOBInline(boolean on) throws SocketException { delegate.setOOBInline(on); }
        @Override public boolean getOOBInline() throws SocketException { return delegate.getOOBInline(); }
        @Override public void setSoTimeout(int timeout) throws SocketException { delegate.setSoTimeout(timeout); }
        @Override public int getSoTimeout() throws SocketException { return delegate.getSoTimeout(); }
        @Override public void setSendBufferSize(int size) throws SocketException { delegate.setSendBufferSize(size); }
        @Override public int getSendBufferSize() throws SocketException { return delegate.getSendBufferSize(); }
        @Override public void setReceiveBufferSize(int size) throws SocketException { delegate.setReceiveBufferSize(size); }
        @Override public int getReceiveBufferSize() throws SocketException { return delegate.getReceiveBufferSize(); }
        @Override public void setKeepAlive(boolean on) throws SocketException { delegate.setKeepAlive(on); }
        @Override public boolean getKeepAlive() throws SocketException { return delegate.getKeepAlive(); }
        @Override public void setTrafficClass(int tc) throws SocketException { delegate.setTrafficClass(tc); }
        @Override public int getTrafficClass() throws SocketException { return delegate.getTrafficClass(); }
        @Override public void setReuseAddress(boolean on) throws SocketException { delegate.setReuseAddress(on); }
        @Override public boolean getReuseAddress() throws SocketException { return delegate.getReuseAddress(); }
        @Override public void close() throws IOException { delegate.close(); }
        @Override public void shutdownInput() throws IOException { delegate.shutdownInput(); }
        @Override public void shutdownOutput() throws IOException { delegate.shutdownOutput(); }
        @Override public boolean isConnected() { return delegate.isConnected(); }
        @Override public boolean isBound() { return delegate.isBound(); }
        @Override public boolean isClosed() { return delegate.isClosed(); }
        @Override public boolean isInputShutdown() { return delegate.isInputShutdown(); }
        @Override public boolean isOutputShutdown() { return delegate.isOutputShutdown(); }
        @Override public void setPerformancePreferences(int connectionTime, int latency, int bandwidth) {
            delegate.setPerformancePreferences(connectionTime, latency, bandwidth);
        }
        @Override public <T> Socket setOption(SocketOption<T> name, T value) throws IOException {
            delegate.setOption(name, value);
            return this;
        }
        @Override public <T> T getOption(SocketOption<T> name) throws IOException { return delegate.getOption(name); }
        @Override public Set<SocketOption<?>> supportedOptions() { return delegate.supportedOptions(); }
        @Override public String toString() { return delegate + " (TLS peer port " + reportedPort + ")"; }
    }
}
