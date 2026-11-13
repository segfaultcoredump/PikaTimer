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

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Iterator;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalDouble;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Parsing and formatting for the Trident TimeMachine text protocol.
 *
 * Pure functions, no I/O. Built from the public Trident manual and the
 * open source tm-csv-connector client (github.com/louking/tm-csv-connector),
 * without access to a unit.
 *
 * TODO verify against a real unit:
 *  - the hundredths field is hex (as in IPICO and tm-csv-connector) and not decimal
 *  - the counter / padding columns do not shift the field positions
 *  - the key names in the summary JSON (battery voltage, temperature)
 *  - the exact $GETDATE / $SETDATE / $REPLAY reply text
 *
 * @author John Garner
 */
final class TridentProtocol {

    static final int DEFAULT_DATA_PORT = 10001;     // filtered data (first / best / last seen)
    static final int DEFAULT_COMMAND_PORT = 9999;   // telnet style commands

    // aa + reader + receiver + chip(12) + counter(4) + YYMMDD + HHMMSS + hh(hex) + 00 [+ type(2) [+ rssi(2)]]
    private static final int MIN_TAG_LINE_LENGTH = 36;

    private static final DateTimeFormatter COMMAND_DATE = DateTimeFormatter.ofPattern("yyMMddHHmmss");
    // DATE:Mon Aug 31 2015 13:17:00 (the day may or may not be padded)
    private static final DateTimeFormatter GETDATE_FORMAT = DateTimeFormatter.ofPattern("EEE MMM d yyyy HH:mm:ss", Locale.ENGLISH);

    private TridentProtocol() {
    }

    /**
     * A tag read from the unit.
     *
     * @param readerId one character reader ID (setting p30, default 'a')
     * @param receiver receiver (mat / antenna) number
     * @param chip the 12 character tag ID
     * @param time the unit's local date / time of the read
     * @param type FS (first seen), BS (best seen), LS (last seen), RR (raw), or "" if not sent
     * @param rssi signal strength, or -1 if not sent
     */
    record TridentRead(char readerId, char receiver, String chip, LocalDateTime time, String type, int rssi) {}

    /**
     * Status from the unit's periodic summary JSON line (off by default on the unit).
     *
     * @param batteryVoltage the battery voltage, or -1 if not in the message
     * @param temperature the temperature, or NaN if not in the message
     */
    record TridentSummary(double batteryVoltage, double temperature) {}

    static boolean isTagLine(String line) {
        return line != null && line.startsWith("aa");
    }

    static boolean isTriggerLine(String line) {
        return line != null && line.startsWith("ab");
    }

    static boolean isSummaryLine(String line) {
        return line != null && line.startsWith("{");
    }

    /**
     * Parses an "aa" tag read line, e.g.
     * aaa1058001234567000124111306123045320000FS2A
     *
     * @return the read, or empty if the line is not a well formed tag read
     */
    static Optional<TridentRead> parseTagLine(String line) {
        if (!isTagLine(line)) return Optional.empty();
        line = line.trim();
        if (line.length() < MIN_TAG_LINE_LENGTH) return Optional.empty();

        try {
            char readerId = line.charAt(2);
            char receiver = line.charAt(3);
            String chip = line.substring(4, 16).trim();
            if (chip.isEmpty()) return Optional.empty();

            int year = 2000 + Integer.parseInt(line.substring(20, 22));
            int month = Integer.parseInt(line.substring(22, 24));
            int day = Integer.parseInt(line.substring(24, 26));
            int hour = Integer.parseInt(line.substring(26, 28));
            int minute = Integer.parseInt(line.substring(28, 30));
            int second = Integer.parseInt(line.substring(30, 32));
            int hundredths = Integer.parseInt(line.substring(32, 34), 16);
            if (hundredths > 99) return Optional.empty();

            LocalDateTime time = LocalDateTime.of(year, month, day, hour, minute, second, hundredths * 10_000_000);

            String type = line.length() >= 38 ? line.substring(36, 38) : "";
            int rssi = -1;
            if (line.length() >= 40) {
                try {
                    rssi = Integer.parseInt(line.substring(38, 40), 16);
                } catch (NumberFormatException ex) {
                    rssi = -1;
                }
            }
            return Optional.of(new TridentRead(readerId, receiver, chip, time, type, rssi));
        } catch (NumberFormatException | DateTimeException ex) {
            return Optional.empty();
        }
    }

    /**
     * Parses the periodic summary JSON line. The exact layout is not
     * published, so the battery voltage is any numeric key containing "batt"
     * or "volt", and the temperature any numeric key containing "temp".
     *
     * @return the summary, or empty if the line is not a JSON object
     */
    static Optional<TridentSummary> parseSummary(String line) {
        if (!isSummaryLine(line)) return Optional.empty();
        JSONObject json;
        try {
            json = new JSONObject(line);
        } catch (JSONException ex) {
            return Optional.empty();
        }
        double battery = findNumber(json, "batt", "volt").orElse(-1);
        double temperature = findNumber(json, "temp").orElse(Double.NaN);
        return Optional.of(new TridentSummary(battery, temperature));
    }

    // The first numeric value whose key contains one of the fragments (case insensitive),
    // looking one level into nested objects too
    private static OptionalDouble findNumber(JSONObject json, String... keyFragments) {
        Iterator<String> keys = json.keys();
        while (keys.hasNext()) {
            String key = keys.next();
            Object value = json.opt(key);
            if (value instanceof JSONObject nested) {
                OptionalDouble found = findNumber(nested, keyFragments);
                if (found.isPresent()) return found;
                continue;
            }
            String lower = key.toLowerCase(Locale.ROOT);
            for (String fragment : keyFragments) {
                if (!lower.contains(fragment)) continue;
                if (value instanceof Number n) return OptionalDouble.of(n.doubleValue());
                if (value instanceof String s) {
                    try {
                        // e.g. "12.4V"
                        return OptionalDouble.of(Double.parseDouble(s.replaceAll("[^0-9.\\-]", "")));
                    } catch (NumberFormatException ex) {
                        // not a number, keep looking
                    }
                }
            }
        }
        return OptionalDouble.empty();
    }

    /**
     * Parses the reply to $GETDATE, e.g. "DATE:Mon Aug 31 2015 13:17:00".
     *
     * @return the unit's date / time, or empty if the line isn't a DATE reply
     */
    static Optional<LocalDateTime> parseGetDate(String line) {
        if (line == null) return Optional.empty();
        line = line.trim();
        int idx = line.toUpperCase(Locale.ROOT).indexOf("DATE:");
        if (idx < 0) return Optional.empty();
        String value = line.substring(idx + 5).trim().replaceAll("\\s+", " ");
        try {
            return Optional.of(LocalDateTime.parse(value, GETDATE_FORMAT));
        } catch (DateTimeParseException ex) {
            return Optional.empty();
        }
    }

    // Is this line the end of a command reply?
    static boolean isFinalReply(String line) {
        if (line == null) return false;
        String l = line.trim().toUpperCase(Locale.ROOT);
        return l.endsWith("OK") || l.contains("UNHANDLED") || l.startsWith("DATE:") || l.startsWith("ERR");
    }

    // Did the unit reject the command?
    static boolean isErrorReply(String line) {
        if (line == null) return false;
        String l = line.trim().toUpperCase(Locale.ROOT);
        return l.contains("UNHANDLED") || l.startsWith("ERR");
    }

    static String getDateCommand() {
        return "$GETDATE";
    }

    static String setDateCommand(LocalDateTime time) {
        return "$SETDATE:" + time.format(COMMAND_DATE);
    }

    // Replay the filtered log from a date / time to now
    static String replayCommand(LocalDateTime from) {
        return "$REPLAY " + from.format(COMMAND_DATE);
    }

    // Replay the whole filtered log
    static String replayAllCommand() {
        return "$REPLAY";
    }

    static String replayStopCommand() {
        return "$REPLAY stop";
    }
}
