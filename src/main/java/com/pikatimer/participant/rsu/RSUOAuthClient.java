/*
 * Copyright (C) 2025 John Garner <segfaultcoredump@gmail.com>
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
package com.pikatimer.participant.rsu;

import com.pikatimer.Pikatimer;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javafx.application.Platform;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OAuth2 Authorization Code + PKCE client for RunSignUp.
 *
 * Flow: open the system browser to the RSU authorize page, catch the redirect
 * on a short lived loopback HTTP listener, then redeem the code for tokens.
 *
 * @author John Garner <segfaultcoredump@gmail.com>
 */
public class RSUOAuthClient {

    private static final Logger logger = LoggerFactory.getLogger(RSUOAuthClient.class);

    private static final long LOGIN_TIMEOUT_SECONDS = 300;
    private static final SecureRandom random = new SecureRandom();

    public record TokenResult(String accessToken, String refreshToken, long expiresAtEpoch) { }

    public static class OAuthException extends Exception {
        public OAuthException(String msg) { super(msg); }
    }

    /**
     * Masks a secret/token for logging: first 4 and last 4 chars only.
     */
    public static String mask(String s) {
        if (s == null) return "null";
        if (s.length() <= 10) return "****";
        return s.substring(0, 4) + "…" + s.substring(s.length() - 4);
    }

    public static boolean isConfigured(){
        return RSUConfig.OAUTH_CLIENT_ID != null && !RSUConfig.OAUTH_CLIENT_ID.isBlank();
    }

    /**
     * Run the interactive browser login. Blocks the calling thread (do NOT call
     * from the FX Application thread) until the user completes the login,
     * an error is returned, or the timeout expires.
     */
    public static TokenResult login() throws Exception {

        if (!isConfigured()) throw new OAuthException("RunSignUp OAuth client ID is not configured");

        // PKCE
        String verifier = randomUrlSafe(32);
        String challenge = base64Url(MessageDigest.getInstance("SHA-256").digest(verifier.getBytes(StandardCharsets.US_ASCII)));
        String state = randomUrlSafe(16);
        logger.debug("RSU OAuth: generated PKCE pair (method=S256, verifier length={}, challenge length={})", verifier.length(), challenge.length());
        logger.debug("RSU OAuth: state={}", state);

        URI redirect = URI.create(RSUConfig.OAUTH_REDIRECT_URI);
        String host = redirect.getHost();
        int port = redirect.getPort() > 0 ? redirect.getPort() : 80;
        String path = (redirect.getPath() == null || redirect.getPath().isEmpty()) ? "/" : redirect.getPath();

        CompletableFuture<String> codeFuture = new CompletableFuture<>();

        HttpServer server = HttpServer.create(new InetSocketAddress(host, port), 0);
        server.createContext(path, exchange -> {
            Map<String,String> params = parseQuery(exchange.getRequestURI().getRawQuery());
            Map<String,String> loggable = new HashMap<>(params);
            if (loggable.containsKey("code")) loggable.put("code", mask(loggable.get("code")));
            logger.debug("RSU OAuth: callback received with params {}", loggable);

            String message;
            if (params.containsKey("error")) {
                logger.debug("RSU OAuth: callback error={} description={}", params.get("error"), params.get("error_description"));
                message = "RunSignUp login failed: " + params.get("error") +
                        (params.containsKey("error_description") ? " (" + params.get("error_description") + ")" : "");
                codeFuture.completeExceptionally(new OAuthException(message));
            } else if (!state.equals(params.get("state"))) {
                logger.debug("RSU OAuth: state mismatch! expected={} received={}", state, params.get("state"));
                message = "RunSignUp login failed: invalid state returned.";
                codeFuture.completeExceptionally(new OAuthException(message));
            } else if (params.get("code") == null || params.get("code").isBlank()) {
                logger.debug("RSU OAuth: callback missing authorization code");
                message = "RunSignUp login failed: no authorization code returned.";
                codeFuture.completeExceptionally(new OAuthException(message));
            } else {
                logger.debug("RSU OAuth: state matched, authorization code received");
                message = "RunSignUp login complete. You may close this window and return to PikaTimer.";
                codeFuture.complete(params.get("code"));
            }

            byte[] body = ("<html><head><title>PikaTimer</title></head><body style=\"font-family:sans-serif\"><h3>"
                    + message.replace("<", "&lt;") + "</h3></body></html>").getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(body); }
        });
        server.start();
        logger.debug("RSU OAuth: loopback listener started on {}:{}{}", host, port, path);

        try {
            StringBuilder authURL = new StringBuilder(RSUConfig.OAUTH_AUTHORIZE_URL);
            authURL.append("?response_type=code");
            authURL.append("&client_id=").append(enc(RSUConfig.OAUTH_CLIENT_ID));
            authURL.append("&redirect_uri=").append(enc(RSUConfig.OAUTH_REDIRECT_URI));
            authURL.append("&scope=").append(enc(RSUConfig.OAUTH_SCOPE));
            authURL.append("&state=").append(enc(state));
            authURL.append("&code_challenge=").append(enc(challenge));
            authURL.append("&code_challenge_method=S256");

            logger.debug("RSU OAuth: opening browser to {}", authURL);
            String url = authURL.toString();
            Platform.runLater(() -> Pikatimer.getInstance().getHostServices().showDocument(url));

            String code;
            try {
                code = codeFuture.get(LOGIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (TimeoutException te) {
                logger.debug("RSU OAuth: timed out after {}s waiting for the login callback", LOGIN_TIMEOUT_SECONDS);
                throw new OAuthException("Timed out waiting for the RunSignUp login");
            } catch (java.util.concurrent.ExecutionException ee) {
                throw (ee.getCause() instanceof Exception ex) ? ex : ee;
            }

            Map<String,String> form = new LinkedHashMap<>();
            form.put("grant_type", "authorization_code");
            form.put("client_id", RSUConfig.OAUTH_CLIENT_ID);
            if (RSUConfig.OAUTH_CLIENT_SECRET != null && !RSUConfig.OAUTH_CLIENT_SECRET.isBlank())
                form.put("client_secret", RSUConfig.OAUTH_CLIENT_SECRET);
            form.put("code", code);
            form.put("redirect_uri", RSUConfig.OAUTH_REDIRECT_URI);
            form.put("code_verifier", verifier);

            return tokenRequest(RSUConfig.OAUTH_TOKEN_URL, form, null);
        } finally {
            server.stop(1);
            logger.debug("RSU OAuth: loopback listener stopped");
        }
    }

    /**
     * Exchange a refresh token for a new access token.
     */
    public static TokenResult refresh(String refreshToken) throws Exception {
        if (refreshToken == null || refreshToken.isBlank()) throw new OAuthException("No refresh token available");

        Map<String,String> form = new LinkedHashMap<>();
        form.put("grant_type", "refresh_token");
        form.put("refresh_token", refreshToken);
        form.put("client_id", RSUConfig.OAUTH_CLIENT_ID);
        if (RSUConfig.OAUTH_CLIENT_SECRET != null && !RSUConfig.OAUTH_CLIENT_SECRET.isBlank())
            form.put("client_secret", RSUConfig.OAUTH_CLIENT_SECRET);

        return tokenRequest(RSUConfig.OAUTH_REFRESH_URL, form, refreshToken);
    }

    private static TokenResult tokenRequest(String endpoint, Map<String,String> form, String previousRefreshToken) throws Exception {
        StringBuilder body = new StringBuilder();
        form.forEach((k,v) -> {
            if (body.length() > 0) body.append('&');
            body.append(enc(k)).append('=').append(enc(v));
        });

        logger.debug("RSU OAuth: token request to {} grant_type={}", endpoint, form.get("grant_type"));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(endpoint))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .build();

        HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        logger.debug("RSU OAuth: token response status {}", response.statusCode());

        if (response.statusCode() != 200) {
            logger.debug("RSU OAuth: token error body: {}", response.body());
            String err = "HTTP " + response.statusCode();
            try {
                JSONObject j = new JSONObject(response.body());
                err = j.optString("error", err) + (j.has("error_description") ? ": " + j.optString("error_description") : "");
            } catch (Exception ignored) { }
            throw new OAuthException("RunSignUp token request failed: " + err);
        }

        JSONObject json = new JSONObject(response.body());
        String access = json.optString("access_token", null);
        if (access == null || access.isBlank()) {
            logger.debug("RSU OAuth: token response missing access_token: {}", response.body());
            throw new OAuthException("RunSignUp token response did not include an access token");
        }
        String refreshTok = json.optString("refresh_token", null);
        if (refreshTok == null || refreshTok.isBlank()) refreshTok = previousRefreshToken;
        long expiresIn = json.optLong("expires_in", 2592000L);
        long expiresAt = Instant.now().getEpochSecond() + expiresIn;

        logger.debug("RSU OAuth: access_token={} refresh_token={} expires_in={} expiresAt={}",
                mask(access), mask(refreshTok), expiresIn, Instant.ofEpochSecond(expiresAt));

        return new TokenResult(access, refreshTok, expiresAt);
    }

    /**
     * Best effort lookup of the logged in RSU user's email/name.
     * Returns "" if it can't be determined.
     */
    public static String lookupUser(String accessToken) {
        try {
            String[] parts = accessToken.split("\\.");
            if (parts.length < 2) {
                logger.debug("RSU OAuth: access token is not a JWT, unable to determine user");
                return "";
            }
            JSONObject claims = new JSONObject(new String(Base64.getUrlDecoder().decode(padBase64(parts[1])), StandardCharsets.UTF_8));
            logger.debug("RSU OAuth: JWT claims present: {}", claims.keySet());

            String userID = null;
            for (String k : new String[]{"user_id", "uid", "sub"}) {
                if (claims.has(k) && !claims.optString(k).isBlank()) { userID = claims.optString(k); break; }
            }
            logger.debug("RSU OAuth: user id from token: {}", userID);

            if (userID != null && userID.matches("^\\d+$")) {
                String url = "https://runsignup.com/Rest/user/" + userID + "?format=json";
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .header("Authorization", "Bearer " + accessToken)
                        .build();
                HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
                logger.debug("RSU OAuth: user lookup {} -> status {}", url, response.statusCode());
                if (response.statusCode() == 200) {
                    JSONObject json = new JSONObject(response.body());
                    JSONObject user = json.optJSONObject("user") != null ? json.getJSONObject("user") : json;
                    String email = user.optString("email", "");
                    String name = (user.optString("first_name", "") + " " + user.optString("last_name", "")).trim();
                    String result = !email.isBlank() ? email : name;
                    logger.debug("RSU OAuth: resolved user: {}", result);
                    if (!result.isBlank()) return result;
                } else {
                    logger.debug("RSU OAuth: user lookup failed: {}", response.body());
                }
            }
            // fall back to any identifying claim in the token itself
            for (String k : new String[]{"email", "username", "name"}) {
                if (claims.has(k) && !claims.optString(k).isBlank()) {
                    logger.debug("RSU OAuth: using '{}' claim as user: {}", k, claims.optString(k));
                    return claims.optString(k);
                }
            }
            return userID == null ? "" : "RSU User " + userID;
        } catch (Exception ex) {
            logger.debug("RSU OAuth: unable to determine user: {}", ex.getMessage());
            return "";
        }
    }

    // Utility bits
    private static String enc(String s) {
        return URLEncoder.encode(s == null ? "" : s, StandardCharsets.UTF_8);
    }

    private static String randomUrlSafe(int bytes) {
        byte[] b = new byte[bytes];
        random.nextBytes(b);
        return base64Url(b);
    }

    private static String base64Url(byte[] b) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b);
    }

    private static String padBase64(String s) {
        int pad = (4 - s.length() % 4) % 4;
        return s + "=".repeat(pad);
    }

    private static Map<String,String> parseQuery(String rawQuery) {
        Map<String,String> map = new HashMap<>();
        if (rawQuery == null || rawQuery.isEmpty()) return map;
        for (String pair : rawQuery.split("&")) {
            int i = pair.indexOf('=');
            String k = URLDecoder.decode(i < 0 ? pair : pair.substring(0, i), StandardCharsets.UTF_8);
            String v = i < 0 ? "" : URLDecoder.decode(pair.substring(i + 1), StandardCharsets.UTF_8);
            map.put(k, v);
        }
        return map;
    }
}
