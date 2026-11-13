/*
 * Copyright (C) 2024 John Garner <segfaultcoredump@gmail.com>
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

import org.pikatimer.PikaPreferences;
import org.pikatimer.participant.Participant;
import org.pikatimer.participant.ParticipantDAO;
import org.pikatimer.race.RaceDAO;
import org.pikatimer.results.ProcessedResult;
import org.pikatimer.results.ResultsDAO;
import org.pikatimer.timing.reader.SelectTimerService;
import io.javalin.Javalin;
import io.javalin.http.staticfiles.Location;
//import static io.javalin.apibuilder.ApiBuilder.get;
//import static io.javalin.apibuilder.ApiBuilder.path;
import io.javalin.websocket.WsContext;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import javafx.concurrent.Task;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.LoggerFactory;

/**
 *
 * @author John Garner <segfaultcoredump@gmail.com>
 */
public class HTTPServices {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(HTTPServices.class);

    private static final List<WsContext> wsSessionList = new ArrayList();
    private Integer port = 8080;
    private Integer httpsPort = -1;
    private final Javalin server;
    private String url = "Not Available";
    
    // Where the select timer web app's files are served from
    public static final String SELECT_TIMER_ASSETS = "/select/assets";
    
    // HTTPS (for the select timer web app)
    private volatile Server jettyServer = null;
    private SslContextFactory.Server sslContextFactory = null;
    private volatile SelfSignedCertificate certificate = null;
    private static final BlockingQueue<String> eventQueue = new ArrayBlockingQueue(100000);
    
    private static Map<WsContext,Set<String>> announcerDupeCheckHash = new HashMap();
    private static Set<String> announcerDupeCheckSet = new HashSet();
    
    /**
    * SingletonHolder is loaded on the first execution of Singleton.getInstance() 
    * or the first access to SingletonHolder.INSTANCE, not before.
    */
    private static class SingletonHolder { 
            private static final HTTPServices INSTANCE = new HTTPServices();
    }

    public static HTTPServices getInstance() {
        
            return SingletonHolder.INSTANCE;
    }
    
    public HTTPServices() {
        Boolean bound = false; 
        port = 8080;
        
        // InetAddress localhost; 
        String ip = "127.0.0.1";
        
        try (final DatagramSocket socket = new DatagramSocket()) {
            // Connect to a pseudo-address to force network routing selection
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            ip = socket.getLocalAddress().getHostAddress();
            System.out.println("Preferred Local IPv4 Address: " + ip);
        } catch (Exception e) {
            e.printStackTrace();
        }
            
            //localhost = InetAddress.getLocalHost();
            logger.info("System IP Address : " + ip); 
            url = "http://" + ip;
        
        server = Javalin.create(config -> {
            // The select timer web app (page, scripts, OCR runtime and models).
            // Everything is served from here since there may be no internet.
            // The OCR runtime and models are large and rarely change: let the
            // devices cache them for a day. (Not precompress, Javalin won't
            // precompress files over 2 MB; they are compressed as they are sent.)
            config.staticFiles.add(staticFiles -> {
                staticFiles.hostedPath = SELECT_TIMER_ASSETS + "/lib";
                staticFiles.directory = "/web/select/lib";
                staticFiles.location = Location.CLASSPATH;
                staticFiles.headers = Map.of(
                        "Cache-Control", "max-age=86400",
                        "Cross-Origin-Opener-Policy", "same-origin",
                        "Cross-Origin-Embedder-Policy", "require-corp",
                        "Cross-Origin-Resource-Policy", "same-origin");
            });
            // The app itself: always check for a newer copy
            config.staticFiles.add(staticFiles -> {
                staticFiles.hostedPath = SELECT_TIMER_ASSETS;
                staticFiles.directory = "/web/select";
                staticFiles.location = Location.CLASSPATH;
                staticFiles.headers = Map.of(
                        "Cache-Control", "no-cache",
                        "Cross-Origin-Opener-Policy", "same-origin",
                        "Cross-Origin-Embedder-Policy", "require-corp",
                        "Cross-Origin-Resource-Policy", "same-origin");
            });
            // Keep a handle on the Jetty server so the HTTPS connector can be added
            config.jetty.modifyServer(s -> jettyServer = s);
        });
        
        // Routes go in before the server is started
        setupHTTPDRoutes();
        SelectTimerService.getInstance().addRoutes(server);
        
        //server.enableCaseSensitiveUrls();
        
        // Lets start at 8080 and just walk up from there until we find a free port
        // but call it quits at 9,000. Look for the port first: a Javalin server
        // can't be started again after a failed start.
        port = findFreePort(8080, 9000, -1);
        if (port > 0) {
            try {
                server.start(port);
                bound = true;
                url += ":" + port;
            } catch (Exception e) {
                logger.error("Unable to start the web server on port " + port, e);
            }
        } else {
            logger.error("Unable to find a free port for the web server");
        }
        
        logger.info("Web server listening on " + url); 
        
        startHTTPS();
        startDiscoveryListener();
        startEventQueueProcessor();
    
    }
    
    public Integer port(){
        return port;
    }
    
    /**
     * @return the HTTPS port, or -1 if HTTPS is not available
     */
    public Integer httpsPort(){
        return httpsPort;
    }
    
    public String getUrl(){
        return url;
    }
    
    public void stopHTTPService(){
        server.stop();
    }
    
    /**
     * This computer's IPv4 addresses, skipping loopback and interfaces that
     * are down. Unlike the "preferred" address above this works when there is
     * no route to the internet.
     *
     * @return the addresses, or 127.0.0.1 if there are none
     */
    public static List<String> getLocalIPv4Addresses() {
        List<String> addresses = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces != null && interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                try {
                    if (networkInterface.isLoopback() || !networkInterface.isUp()) continue;
                } catch (SocketException ex) {
                    continue;
                }
                for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
                    InetAddress address = interfaceAddress.getAddress();
                    if (address instanceof Inet4Address && !address.isLinkLocalAddress()) {
                        addresses.add(address.getHostAddress());
                    }
                }
            }
        } catch (SocketException ex) {
            logger.warn("Unable to list the network interfaces", ex);
        }
        if (addresses.isEmpty()) addresses.add("127.0.0.1");
        return addresses;
    }
    
    /**
     * @return the first port in [from, to) that is free on all addresses
     * (and isn't skip), or -1
     */
    private static int findFreePort(int from, int to, int skip) {
        for (int p = from; p < to; p++) {
            if (p == skip) continue;
            try (ServerSocket socket = new ServerSocket()) {
                socket.setReuseAddress(true);
                socket.bind(new InetSocketAddress(p));
                return p;
            } catch (IOException ex) {
                logger.trace("Port {} is in use", p);
            }
        }
        return -1;
    }
    
    // Add an HTTPS connector to the running server. This is done after the
    // plain HTTP side is up so a certificate problem can't take it down.
    private void startHTTPS() {
        if (jettyServer == null || !jettyServer.isStarted()) return;
        try {
            certificate = SelfSignedCertificate.load(getLocalIPv4Addresses());
            sslContextFactory = new SslContextFactory.Server();
            sslContextFactory.setKeyStore(certificate.getKeyStore());
            sslContextFactory.setKeyStorePassword(certificate.getPassword());
            sslContextFactory.setCertAlias(SelfSignedCertificate.ALIAS);
            
            // Same settings as the HTTP connector, plus the TLS details. Devices
            // connect by IP address, so there is no SNI host to check.
            HttpConfiguration httpsConfig = new HttpConfiguration();
            for (Connector c : jettyServer.getConnectors()) {
                HttpConnectionFactory f = c.getConnectionFactory(HttpConnectionFactory.class);
                if (f != null) {
                    httpsConfig = new HttpConfiguration(f.getHttpConfiguration());
                    break;
                }
            }
            SecureRequestCustomizer secure = new SecureRequestCustomizer();
            secure.setSniHostCheck(false);
            secure.setSniRequired(false);
            httpsConfig.addCustomizer(secure);
            
            for (int p = findFreePort(8443, 9500, port); p > 0; p = findFreePort(p + 1, 9500, port)) {
                ServerConnector connector = new ServerConnector(jettyServer,
                        new SslConnectionFactory(sslContextFactory, "http/1.1"),
                        new HttpConnectionFactory(httpsConfig));
                connector.setPort(p);
                try {
                    jettyServer.addConnector(connector);
                    connector.start();
                    httpsPort = p;
                    logger.info("HTTPS listening on port {} (certificate {})", p, certificate.getFingerprint());
                    return;
                } catch (Exception ex) {
                    jettyServer.removeConnector(connector);
                    try { connector.stop(); } catch (Exception ignored) {}
                    logger.trace("HTTPS port {} is not available: {}", p, ex.getMessage());
                }
            }
            logger.warn("Unable to find a free port for HTTPS");
        } catch (Exception ex) {
            logger.error("Unable to start HTTPS", ex);
        }
    }
    
    /**
     * Make sure the HTTPS certificate covers this computer's current IP
     * addresses (e.g. after joining the network at the race site). A new
     * certificate means each device has to accept it again.
     */
    public synchronized void refreshCertificate() {
        if (sslContextFactory == null || certificate == null) return;
        List<String> addresses = getLocalIPv4Addresses();
        if (certificate.covers(addresses)) return;
        try {
            SelfSignedCertificate updated = SelfSignedCertificate.load(addresses);
            sslContextFactory.reload(f -> {
                f.setKeyStore(updated.getKeyStore());
                f.setKeyStorePassword(updated.getPassword());
            });
            certificate = updated;
        } catch (Exception ex) {
            logger.warn("Unable to update the HTTPS certificate", ex);
        }
    }
    
    /**
     * @return the SHA-256 fingerprint of the HTTPS certificate, or "" if there is none
     */
    public String getCertificateFingerprint() {
        return certificate == null ? "" : certificate.getFingerprint();
    }
    
    
    
    private void startDiscoveryListener(){
        // Setup a network discovery listener so others can find us
        // Borrowed from https://michieldemey.be/blog/network-discovery-using-udp-broadcast/
        Task discoveryThread = new Task<Void>() {
            @Override public Void call() {
                try {

                  //Keep a socket open to listen to all the UDP trafic that is destined for this port
                  DatagramSocket socket = new DatagramSocket(8080, InetAddress.getByName("0.0.0.0"));
                  socket.setBroadcast(true);

                    while (true) {
                        logger.info("DiscoveryListener:  Ready to receive broadcast packets!");
                        //Receive a packet
                        byte[] recvBuf = new byte[15000];
                        DatagramPacket packet = new DatagramPacket(recvBuf, recvBuf.length);
                        socket.receive(packet);

                        //Packet received
                        logger.debug(">>>Discovery packet received from: " + packet.getAddress().getHostAddress());
                        logger.debug(">>>Packet received; data: " + new String(packet.getData()).trim());
                        //See if the packet holds the right command (message)
                        String message = new String(packet.getData()).trim();
                        if (message.equals("DISCOVER_PIKA_REQUEST") && PikaPreferences.getInstance().getDBLoaded() ) {
                          byte[] sendData = url.getBytes();
                          DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, packet.getAddress(), packet.getPort());
                          socket.send(sendPacket);
                          logger.debug(">>>Sent packet to: " + sendPacket.getAddress().getHostAddress());
                        }
                    }
                } catch (IOException ex) {
                    logger.debug("Discovery Listener Exception",ex);
                }
                return null;
            }

        
        };
        Thread discovery = new Thread(discoveryThread);
        discovery.setDaemon(true);
        discovery.setName("PikaAnnouncer Discovery Listener Thread");
        discovery.start();
    }
    
    public void publishEvent(String category, JSONObject event){
        logger.debug("WebSocket Publish Event: " + category + ":" + event);
        
        eventQueue.add(new JSONObject().put(category, event).toString());
    }
    
    public void publishEvent(String category, String event){
        
        logger.debug("WebSocket Publish Event: " + category + ":" + event);
        
        eventQueue.add(new JSONObject().put(category, event).toString());
        
    }
    
     private void startEventQueueProcessor(){

        Task eventThread = new Task<Void>() {
            @Override public Void call() {
                while(true) {
                    String save = "";
                    try {
                        while(true) {
                            logger.debug("HTTPServices: Waiting for events to publish");
                            String m = eventQueue.poll(20, TimeUnit.SECONDS);
                            if (m == null) {
                                m = "{\"KEEPALIVE\":\"" + System.currentTimeMillis() + "\"}";
                            }
                            String message = m;
                            logger.debug("HTTPServices: Publishing Event");
                            wsSessionList.stream().forEach(ctx -> {
                                if(message.contains("PARTICIPANT") || message.contains("KEEPALIVE") || ! announcerDupeCheckHash.get(ctx).contains(message)) {
                                    announcerDupeCheckHash.get(ctx).add(message);
                                    try {
                                        logger.debug(" HTTPServices: Publishing Event  to " + ctx.sessionId() + " " + ctx.host());
                                        ctx.send(message);
                                        logger.trace(" HTTPServices: Successfuly published to " + ctx.sessionId() + " " + ctx.host());
                                    } catch (Exception e){
                                        eventQueue.add(message);
                                        logger.warn("Event Processor Exception: " + e.getMessage());
                                    }     
                                }
                            });
                        }
                    } catch (Exception ex) {
                        logger.warn("Event Processor Outer Exception: " + ex.getMessage());
                    }
                    
                    logger.info("Marmot Event Processor Thread Ended!!!");
                    if (!save.isEmpty()) eventQueue.add(save);
                }
            }
        };
        Thread eventProcessor = new Thread(eventThread);
        eventProcessor.setDaemon(true);
        eventProcessor.setName("Marmot Event Processor Thread");
        eventProcessor.start();
    }

    private void setupHTTPDRoutes() {
        
        // Event Websocket
        server.ws("/eventsocket/", ws -> {
            ws.onConnect(ctx -> {
                ctx.enableAutomaticPings();
                
                wsSessionList.add(ctx);
                announcerDupeCheckHash.put(ctx, new HashSet());
                
                logger.debug("WebSocket Connected: " + ctx.host() + " Size: " + wsSessionList.size());
                
            });
            ws.onClose(session -> {
                wsSessionList.remove(session);
            
            });
        });
        
        // Setup the routes
        server.get("/participants/{id}", ctx -> {

                            logger.debug("Requesting participant with bib " + ctx.pathParam("id"));
                            Participant p = ParticipantDAO.getInstance().getParticipantByBib(ctx.pathParam("id"));
                            if (p==null) {
                                logger.debug("No Participant found!");
                                ctx.status(404);
                                ctx.result("NOT_FOUND");
                            }
                            else {
                                ctx.json(p.getJSONObject().toString());
                            }
        });
        server.get("/participants", ctx -> {
            JSONArray p = new JSONArray();
            JSONObject o = new JSONObject();
            logger.debug("HTTPServices.java: /participants called");
            ParticipantDAO.getInstance().listParticipants().forEach(part -> {p.put(part.getJSONObject());});
            o.put("Participants", p);
            //ctx.contentType("application/json; charset=utf-8");
            logger.trace("Returning {}",o.toString(4));
            ctx.json(o.toString(4));
        
        });
        
        server.get("/results",ctx -> {
            logger.debug("HTTPServices.java: /results called");
            JSONArray p = new JSONArray();
            JSONObject o = new JSONObject();
            ResultsDAO resDAO = ResultsDAO.getInstance();
            RaceDAO.getInstance().listRaces().forEach(r -> {
                resDAO.copyResults(r.getID()).forEach(res -> {

                    if (!res.isEmpty() && res.getFinish()>0) {
                        String race = "";
                        if (RaceDAO.getInstance().listRaces().size() > 1) 
                            race = RaceDAO.getInstance().getRaceByID(res.getRaceID()).getRaceName();
                        String bib = res.getBib();
                        //String time = DurationFormatter.durationToString(res.getFinishDuration().minus(res.getStartDuration()), "[HH:]MM:SS");
                        ProcessedResult pr = resDAO.processResult(res,r);
                        String time = DurationFormatter.durationToString(pr.getChipFinish(), "[HH:]MM:SS");

                        JSONObject json = new JSONObject();
                        json.put("Bib", bib);
                        json.put("Race", race);
                        json.put("Time", time);
                        //logger.debug("/ Results -> " + bib + " -> " + time);
                        p.put(json);
                    }
                });
                o.put("Results", p);
            });
            ctx.json(o.toString());
        });
    }
    
    public Javalin getServer(){
        return server;
    }
}
