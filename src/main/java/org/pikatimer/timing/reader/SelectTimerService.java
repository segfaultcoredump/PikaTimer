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

import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import javafx.application.Platform;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.pikatimer.PikaPreferences;
import org.pikatimer.participant.Participant;
import org.pikatimer.participant.ParticipantDAO;
import org.pikatimer.race.RaceDAO;
import org.pikatimer.race.Wave;
import org.pikatimer.timing.TimingDAO;
import org.pikatimer.timing.TimingInputTypes;
import org.pikatimer.timing.TimingLocation;
import org.pikatimer.timing.TimingLocationInput;
import org.pikatimer.timing.reader.SelectTimerReader.DeviceStats;
import org.pikatimer.timing.reader.SelectTimerReader.ReadStatus;
import org.pikatimer.util.HTTPServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The web side of the automatic select timer (SelectTimerReader).
 *
 * Pages and API, all keyed by the input's token:
 * <pre>
 * GET  /select/{token}/            the web app (its files are under /select/assets/)
 * GET  /api/select/{token}/config  input settings
 * GET  /api/select/{token}/bibs    the registered bibs (ETag, so a refresh is cheap)
 * GET  /api/select/{token}/time    this computer's clock, for the device's clock sync (also the heartbeat)
 * POST /api/select/{token}/reads   bibs seen by the device; answered with acks and participant details
 * </pre>
 * Every API call can carry the device's details as query parameters:
 * device (id), name, pending (queued reads), offset and rtt (clock sync, ms), v (app version).
 *
 * Status codes: 404 unknown link, 503 no event open or the input is not
 * accepting reads (the device keeps its reads queued).
 *
 * @author John Garner
 */
public final class SelectTimerService {
    private static final Logger logger = LoggerFactory.getLogger(SelectTimerService.class);

    private static final String PAGE = "/web/select/index.html";
    private static final Pattern TOKEN = Pattern.compile("[a-z0-9]{4,32}");
    // Bibs: letters, numbers and a dash, up to 16 characters
    private static final Pattern BIB = Pattern.compile("[A-Za-z0-9-]{1,16}");
    private static final int MAX_READS_PER_POST = 500;
    // Remember unknown tokens for a bit so a bad link doesn't keep the FX thread busy
    private static final long MISS_CACHE_MS = 5000;

    private final Map<String, SelectTimerReader> readers = new ConcurrentHashMap<>();
    private final Map<String, Long> misses = new ConcurrentHashMap<>();

    private static class SingletonHolder {
        private static final SelectTimerService INSTANCE = new SelectTimerService();
    }

    public static SelectTimerService getInstance() {
        return SingletonHolder.INSTANCE;
    }

    private SelectTimerService() {
    }

    void register(String token, SelectTimerReader reader) {
        if (token == null || token.isEmpty()) return;
        readers.put(token, reader);
        misses.remove(token);
        logger.debug("SelectTimer: registered {}", token);
    }

    void unregister(String token, SelectTimerReader reader) {
        if (token == null) return;
        readers.remove(token, reader);
        logger.debug("SelectTimer: unregistered {}", token);
    }

    // ------------------------------------------------------------------
    // Lookup
    // ------------------------------------------------------------------

    /**
     * Find the reader for a token. Readers are normally started when their
     * timing location is shown in the Timing tab, so if the token is not
     * registered yet look for the input and start its reader.
     */
    private SelectTimerReader find(String token) {
        if (token == null || !TOKEN.matcher(token).matches()) return null;
        SelectTimerReader reader = readers.get(token);
        if (reader != null) return reader;

        Long missed = misses.get(token);
        if (missed != null && System.currentTimeMillis() - missed < MISS_CACHE_MS) return null;

        // The timing locations and inputs belong to the FX thread
        CompletableFuture<SelectTimerReader> result = new CompletableFuture<>();
        Platform.runLater(() -> {
            try {
                result.complete(startReader(token));
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        });
        try {
            reader = result.get(10, TimeUnit.SECONDS);
        } catch (Exception ex) {
            logger.warn("SelectTimer: unable to look up {}", token, ex);
            return null;
        }
        if (reader == null) misses.put(token, System.currentTimeMillis());
        return reader;
    }

    // FX thread
    private SelectTimerReader startReader(String token) {
        SelectTimerReader reader = readers.get(token);
        if (reader != null) return reader;
        for (TimingLocation tl : new ArrayList<>(TimingDAO.getInstance().listTimingLocations())) {
            for (TimingLocationInput tli : new ArrayList<>(tl.getInputs())) {
                if (tli.getTimingInputType() == TimingInputTypes.SelectTimer
                        && token.equals(tli.getAttribute(SelectTimerReader.ATTR_TOKEN))) {
                    logger.info("SelectTimer: starting the reader for {} ({})", tli.getLocationName(), tl.getLocationName());
                    tli.initializeReader();
                    if (tli.getTimingReader() instanceof SelectTimerReader r) {
                        r.startReading();
                        return r;
                    }
                }
            }
        }
        return null;
    }

    // ------------------------------------------------------------------
    // Routes
    // ------------------------------------------------------------------

    /**
     * Add the select timer pages and API. Called before the server starts.
     *
     * @param server the web server
     */
    public void addRoutes(Javalin server) {

        // The camera only works on a secure page, so send the page over to HTTPS
        server.before("/select/*", ctx -> {
            int httpsPort = HTTPServices.getInstance().httpsPort();
            if (!"https".equalsIgnoreCase(ctx.scheme()) && httpsPort > 0
                    && !ctx.path().startsWith(HTTPServices.SELECT_TIMER_ASSETS)) {
                String query = ctx.queryString() == null ? "" : "?" + ctx.queryString();
                ctx.redirect("https://" + hostOnly(ctx.req().getServerName()) + ":" + httpsPort + ctx.path() + query);
                ctx.skipRemainingHandlers();
            }
        });

        // The web app. It is the same page for every token; it shows its own
        // message if the link is not (or no longer) valid.
        server.get("/select/{token}", ctx -> {
            InputStream page = SelectTimerService.class.getResourceAsStream(PAGE);
            if (page == null) {
                ctx.status(500).result("The select timer app is missing from this build");
                return;
            }
            ctx.header("Cache-Control", "no-cache");
            ctx.header("Cross-Origin-Opener-Policy", "same-origin");
            ctx.header("Cross-Origin-Embedder-Policy", "require-corp");
            ctx.contentType("text/html; charset=utf-8");
            ctx.result(page);
        });

        server.get("/api/select/{token}/config", api((ctx, reader, device) -> {
            JSONObject o = new JSONObject();
            o.put("location", reader.getTimingLocationName());
            o.put("input", reader.getInputName());
            o.put("enabled", reader.isEnabled());
            o.put("knownBibsOnly", reader.isKnownBibsOnly());
            o.put("repeatSeconds", reader.getRepeatSeconds());
            o.put("serverTime", System.currentTimeMillis());
            json(ctx, o);
        }));

        server.get("/api/select/{token}/time", api((ctx, reader, device) -> {
            JSONObject o = new JSONObject();
            o.put("serverTime", System.currentTimeMillis());
            o.put("enabled", reader.isEnabled());
            o.put("knownBibsOnly", reader.isKnownBibsOnly());
            o.put("repeatSeconds", reader.getRepeatSeconds());
            json(ctx, o);
        }));

        server.get("/api/select/{token}/bibs", api((ctx, reader, device) -> {
            Set<String> bibs = new TreeSet<>();
            for (Participant p : new ArrayList<>(ParticipantDAO.getInstance().listParticipants())) {
                String bib = p.getBib();
                if (bib != null && !bib.isBlank()) bibs.add(bib.trim());
            }
            String version = hash(String.join("\n", bibs));
            String etag = "\"" + version + "\"";
            ctx.header("ETag", etag);
            ctx.header("Cache-Control", "no-cache");
            if (etag.equals(ctx.header("If-None-Match"))) {
                ctx.status(304);
                return;
            }
            JSONObject o = new JSONObject();
            o.put("version", version);
            o.put("bibs", new JSONArray(bibs));
            json(ctx, o);
        }));

        server.post("/api/select/{token}/reads", api((ctx, reader, device) -> {
            JSONObject body;
            JSONArray reads;
            try {
                body = new JSONObject(ctx.body());
                reads = body.getJSONArray("reads");
            } catch (JSONException ex) {
                ctx.status(400);
                json(ctx, new JSONObject().put("error", "Bad request: " + ex.getMessage()));
                return;
            }
            if (reads.length() > MAX_READS_PER_POST) {
                ctx.status(413);
                json(ctx, new JSONObject().put("error", "Too many reads, send at most " + MAX_READS_PER_POST));
                return;
            }
            if (!reader.isEnabled()) {
                ctx.status(503);
                json(ctx, new JSONObject().put("error", "This input is not accepting reads").put("enabled", false));
                return;
            }

            JSONArray acks = new JSONArray();
            for (int i = 0; i < reads.length(); i++) {
                JSONObject read = reads.optJSONObject(i);
                if (read == null) continue;
                String id = read.optString("id", "");
                String bib = read.optString("bib", "").trim();
                long epochMs = read.optLong("epochMs", -1);

                ReadStatus status;
                if (!BIB.matcher(bib).matches() || epochMs <= 0) status = ReadStatus.INVALID;
                else status = reader.acceptRead(device, bib, epochMs);

                // Stop if the input was turned off part way through; the
                // device keeps the rest queued
                if (status == ReadStatus.DISABLED) break;

                JSONObject ack = new JSONObject();
                ack.put("id", id);
                ack.put("status", status.name().toLowerCase());
                if (status == ReadStatus.OK || status == ReadStatus.DUPLICATE) {
                    Object participant = participantJSON(bib);
                    ack.put("participant", participant);
                }
                acks.put(ack);
            }
            JSONObject o = new JSONObject();
            o.put("acks", acks);
            o.put("enabled", reader.isEnabled());
            o.put("serverTime", System.currentTimeMillis());
            json(ctx, o);
        }));
    }

    @FunctionalInterface
    private interface ApiHandler {
        void handle(Context ctx, SelectTimerReader reader, DeviceStats device) throws Exception;
    }

    // Common API checks: event open, known token, note the device
    private Handler api(ApiHandler handler) {
        return ctx -> {
            ctx.header("Cache-Control", "no-store");
            if (!PikaPreferences.getInstance().getDBLoaded()) {
                ctx.status(503);
                json(ctx, new JSONObject().put("error", "No event is open in PikaTimer"));
                return;
            }
            SelectTimerReader reader = find(ctx.pathParam("token"));
            if (reader == null) {
                ctx.status(404);
                json(ctx, new JSONObject().put("error", "Unknown link. The input may have been deleted or given a new link."));
                return;
            }
            DeviceStats device = reader.deviceContact(
                    limit(ctx.queryParam("device"), 64),
                    limit(ctx.queryParam("name"), 40),
                    displayAddress(ctx.ip()),
                    parseInt(ctx.queryParam("pending")),
                    parseLong(ctx.queryParam("offset")),
                    parseLong(ctx.queryParam("rtt")),
                    limit(ctx.queryParam("v"), 20));
            try {
                handler.handle(ctx, reader, device);
            } catch (Exception ex) {
                logger.warn("SelectTimer: error handling {}", ctx.path(), ex);
                ctx.status(500);
                json(ctx, new JSONObject().put("error", String.valueOf(ex.getMessage())));
            }
        };
    }

    private static Object participantJSON(String bib) {
        Participant p = ParticipantDAO.getInstance().getParticipantByBib(bib);
        if (p == null) return JSONObject.NULL;
        JSONObject o = new JSONObject();
        o.put("bib", p.getBib());
        o.put("first", p.getFirstName());
        o.put("last", p.getLastName());
        o.put("name", (p.getFirstName() + " " + p.getLastName()).trim());
        Integer age = p.getAge();
        if (age != null) o.put("age", age);
        o.put("sex", p.getSex());
        o.put("race", raceName(p));
        return o;
    }

    // The race name, only if there is more than one race
    private static String raceName(Participant p) {
        try {
            RaceDAO raceDAO = RaceDAO.getInstance();
            if (raceDAO.listRaces().size() < 2) return "";
            List<String> names = new ArrayList<>();
            for (Integer id : new ArrayList<>(p.getWaveIDs())) {
                Wave w = raceDAO.getWaveByID(id);
                if (w != null && w.getRace() != null && !names.contains(w.getRace().getRaceName())) {
                    names.add(w.getRace().getRaceName());
                }
            }
            return String.join(", ", names);
        } catch (Exception ex) {
            return "";
        }
    }

    private static void json(Context ctx, JSONObject o) {
        ctx.contentType("application/json; charset=utf-8");
        ctx.result(o.toString());
    }

    private static String hash(String s) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(d, 0, 12);
        } catch (Exception ex) {
            return Integer.toHexString(s.hashCode());
        }
    }

    private static String hostOnly(String host) {
        if (host == null) return "localhost";
        // IPv6 literal
        if (host.contains(":") && !host.startsWith("[")) return "[" + host + "]";
        return host;
    }

    private static String displayAddress(String ip) {
        if (ip == null) return "";
        String a = ip.replace("[", "").replace("]", "");
        if (a.equals("0:0:0:0:0:0:0:1") || a.equals("::1")) return "localhost";
        if (a.startsWith("::ffff:")) return a.substring(7);
        return a;
    }

    private static String limit(String s, int max) {
        if (s == null) return null;
        s = s.trim();
        return s.length() > max ? s.substring(0, max) : s;
    }

    private static Integer parseInt(String s) {
        try {
            return s == null ? null : Integer.valueOf(s.trim());
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private static Long parseLong(String s) {
        try {
            return s == null ? null : Long.valueOf(s.trim());
        } catch (NumberFormatException ex) {
            return null;
        }
    }
}
