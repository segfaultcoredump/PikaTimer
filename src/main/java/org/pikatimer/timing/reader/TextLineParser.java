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
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Turns one line of a text file into a chip/bib and a timestamp.
 *
 * A parser is either a list of columns (the predefined formats, split on
 * whatever delimiter the line uses) or a compiled custom format string. The
 * custom format uses the same escape codes as the PikaDownloader custom output
 * format, so a file written by PikaDownloader can be read back with the same
 * format string:
 *
 * <pre>
 *   B  / BIB         Bib number
 *   C  / CHIP        Chip number
 *   DT / DATETIME    Date and time (2026-01-02 01:02:03.456)
 *   D  / DATE        Date (2026-01-02)
 *   T  / TIME        Time of day (01:02:03.456)
 *   Z  / TZ          Timezone offset, only used with EpochMilli
 *   E  / EPOCHMILLI  Milliseconds since the epoch
 *   A, R, S, X / SKIP  (Antenna, Reader, RSSI, anything else) ignored
 * </pre>
 *
 * Codes start with $ or %, may be wrapped in {} and are case insensitive. The
 * date and time codes take an optional DateTimeFormatter pattern, e.g.
 * %{D:MM/dd/yyyy}. A backslash escapes a code so it is treated as text.
 *
 * Instances are immutable and safe to share between threads.
 *
 * @author John Garner
 */
public final class TextLineParser {

    public enum Field {
        BIB("$B", "Bib"),
        CHIP("$C", "Chip"),
        DATE("$D", "Date"),
        TIME("$T", "Time"),
        DATETIME("$DT", "DateTime"),
        TZ("$Z", "TZ"),
        EPOCH("$E", "EpochMilli"),
        IGNORE("$X", "Skip");

        private final String code;
        private final String label;

        Field(String code, String label) {
            this.code = code;
            this.label = label;
        }

        /**
         * @return the short custom format code for this field (e.g. "$B")
         */
        public String code() {
            return code;
        }

        @Override
        public String toString() {
            return label;
        }
    }

    /**
     * A read parsed from a line.
     *
     * @param id the chip or bib
     * @param timestamp the time since midnight on the event date
     */
    public record ParsedRead(String id, Duration timestamp) {}

    /**
     * Thrown when a line can't be turned into a read.
     */
    public static final class LineParseException extends Exception {
        public LineParseException(String message) {
            super(message);
        }
    }

    // Same token syntax as the PikaDownloader CustomFormat
    // Group 1: the escape backslash
    // Group 2: %TOKEN or $TOKEN
    // Group 3: ${TOKEN} or %{TOKEN}
    // Group 4: the optional :PARAM within ${TOKEN:PARAM}
    private static final Pattern TOKEN_PATTERN = Pattern.compile(
            "(\\\\)?(?:[\\%$]([A-Za-z]+)|[\\%$]\\{([A-Za-z]+)(?::([^}]+))?})",
            Pattern.CASE_INSENSITIVE
    );

    private static final Pattern WHITESPACE = Pattern.compile("\\s+");

    // H:mm:ss[.fffffffff] [AM|PM], hours may run past 24 for multi-day events
    private static final Pattern TIME_PATTERN = Pattern.compile(
            "^(\\d{1,3}):(\\d{2}):(\\d{2})(?:[.,](\\d{1,9}))?(?:\\s*([AaPp])\\.?[Mm]\\.?)?$");

    // A date and a time separated by a space or a T
    private static final Pattern DATE_TIME_SPLIT = Pattern.compile("^(\\S+?)(?:\\s+|T)(\\d.*)$");

    private static final Pattern EPOCH_PATTERN = Pattern.compile("^\\d{12,14}$");

    private static final List<DateTimeFormatter> DATE_FORMATS = List.of(
            DateTimeFormatter.ISO_LOCAL_DATE,
            DateTimeFormatter.ofPattern("M/d/uuuu", Locale.ROOT),
            DateTimeFormatter.ofPattern("uuuu/M/d", Locale.ROOT),
            DateTimeFormatter.ofPattern("M-d-uuuu", Locale.ROOT)
    );

    // ZoneId.of() re-parses the offset every time, so cache them
    private static final Map<String, ZoneId> ZONE_CACHE = new ConcurrentHashMap<>();

    private record Column(Field field, DateTimeFormatter formatter) {}

    private final List<Column> columns;
    private final Pattern linePattern;   // null: split the line on its delimiter
    private final String format;         // what we show the user
    private final String error;          // null if the parser is usable

    private final int bibIdx;
    private final int chipIdx;
    private final int dateIdx;
    private final int timeIdx;
    private final int dateTimeIdx;
    private final int tzIdx;
    private final int epochIdx;

    private TextLineParser(List<Column> columns, Pattern linePattern, String format, String error) {
        this.columns = List.copyOf(columns);
        this.linePattern = linePattern;
        this.format = format;

        bibIdx = indexOf(Field.BIB);
        chipIdx = indexOf(Field.CHIP);
        dateIdx = indexOf(Field.DATE);
        timeIdx = indexOf(Field.TIME);
        dateTimeIdx = indexOf(Field.DATETIME);
        tzIdx = indexOf(Field.TZ);
        epochIdx = indexOf(Field.EPOCH);

        if (error == null) {
            if (bibIdx < 0 && chipIdx < 0) {
                error = "The format needs a Bib ($B) or Chip ($C)";
            } else if (timeIdx < 0 && dateTimeIdx < 0 && epochIdx < 0) {
                error = "The format needs a Time ($T), DateTime ($DT) or EpochMilli ($E)";
            }
        }
        this.error = error;
    }

    private int indexOf(Field f) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).field() == f) return i;
        }
        return -1;
    }

    /**
     * A parser for delimited columns. Each line is split on a tab, comma,
     * semicolon, pipe or whitespace (the first one found in the line).
     *
     * @param fields the columns, in order
     * @return the parser
     */
    public static TextLineParser ofColumns(List<Field> fields) {
        List<Column> cols = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        for (Field f : fields) {
            cols.add(new Column(f, null));
            if (sb.length() > 0) sb.append(',');
            sb.append(f);
        }
        return new TextLineParser(cols, null, sb.toString(), null);
    }

    /**
     * Compile a custom format string. A bad format string does not throw,
     * the returned parser's isValid() is false and error() says why.
     *
     * @param format the custom format
     * @return the parser
     */
    public static TextLineParser compileCustom(String format) {
        List<Column> cols = new ArrayList<>();
        if (format == null || format.isBlank()) {
            return new TextLineParser(cols, null, "", "Enter a custom format, e.g. $C,$D,$T");
        }

        StringBuilder regex = new StringBuilder("^\\s*");
        Matcher matcher = TOKEN_PATTERN.matcher(format);
        int last = 0;
        String error = null;
        while (matcher.find()) {
            if (matcher.start() > last) {
                appendLiteral(regex, format.substring(last, matcher.start()));
            }
            last = matcher.end();

            // Escaped: strip the backslash and match the rest as text
            if (matcher.group(1) != null) {
                appendLiteral(regex, matcher.group().substring(1));
                continue;
            }

            String tokenName;
            String parameter = null;
            if (matcher.group(2) != null) {
                tokenName = matcher.group(2).toLowerCase(Locale.ROOT);
            } else {
                tokenName = matcher.group(3).toLowerCase(Locale.ROOT);
                parameter = matcher.group(4);
            }

            Field field = switch (tokenName) {
                case "b", "bib" -> Field.BIB;
                case "c", "chip" -> Field.CHIP;
                case "d", "date" -> Field.DATE;
                case "t", "time" -> Field.TIME;
                case "dt", "datetime" -> Field.DATETIME;
                case "z", "tz", "timezone" -> Field.TZ;
                case "e", "epochmilli" -> Field.EPOCH;
                case "a", "antenna", "r", "reader", "s", "rssi", "x", "skip" -> Field.IGNORE;
                default -> null;
            };
            if (field == null) {
                if (error == null) error = "Unknown code " + matcher.group() + " (use \\" + matcher.group() + " to match it as text)";
                continue;
            }

            DateTimeFormatter formatter = null;
            if (parameter != null && (field == Field.DATE || field == Field.TIME || field == Field.DATETIME)) {
                try {
                    formatter = DateTimeFormatter.ofPattern(parameter, Locale.ROOT);
                } catch (IllegalArgumentException e) {
                    if (error == null) error = "Invalid date/time pattern \"" + parameter + "\": " + e.getMessage();
                }
            }

            // A field used twice only counts the first time
            for (Column c : cols) {
                if (c.field() == field && field != Field.IGNORE) {
                    field = Field.IGNORE;
                    break;
                }
            }
            cols.add(new Column(field, formatter));
            regex.append("(.*?)");
        }
        if (last < format.length()) {
            appendLiteral(regex, format.substring(last));
        }
        regex.append("\\s*$");

        Pattern pattern = null;
        if (error == null) {
            try {
                pattern = Pattern.compile(regex.toString());
            } catch (RuntimeException e) {
                error = "Unable to compile the format: " + e.getMessage();
            }
        }
        return new TextLineParser(cols, pattern, format, error);
    }

    // Literal text in a custom format. A \t is a tab. A run of whitespace with
    // a tab in it matches a tab (with optional spaces around it), any other
    // run of whitespace matches one or more whitespace characters, and
    // everything else has to match exactly.
    private static void appendLiteral(StringBuilder regex, String literal) {
        literal = literal.replace("\\t", "\t");
        Matcher ws = WHITESPACE.matcher(literal);
        int last = 0;
        while (ws.find()) {
            if (ws.start() > last) regex.append(Pattern.quote(literal.substring(last, ws.start())));
            regex.append(ws.group().contains("\t") ? "[ ]*\\t[ ]*" : "\\s+");
            last = ws.end();
        }
        if (last < literal.length()) regex.append(Pattern.quote(literal.substring(last)));
    }

    /**
     * @return true if the parser can be used
     */
    public boolean isValid() {
        return error == null;
    }

    /**
     * @return why the parser can't be used, or null if it is valid
     */
    public String error() {
        return error;
    }

    /**
     * @return the format (e.g. "Chip,Date,Time" or the custom format string)
     */
    public String format() {
        return format;
    }

    public boolean hasBib() {
        return bibIdx >= 0;
    }

    public boolean hasChip() {
        return chipIdx >= 0;
    }

    /**
     * @param preferBib true to use the Bib column when the line has both
     * @return true if the id that parse() returns is a bib (so it should not
     * go through the chip -> bib map)
     */
    public boolean idIsBib(boolean preferBib) {
        return hasBib() && (!hasChip() || preferBib);
    }

    /**
     * @param line a line from the file
     * @param eventDate the event date, timestamps are relative to midnight on it
     * @param preferBib true to use the Bib column when the line has both
     * @return the read
     * @throws LineParseException if the line can't be parsed (e.g. a header)
     */
    public ParsedRead parse(String line, LocalDate eventDate, boolean preferBib) throws LineParseException {
        if (error != null) throw new LineParseException(error);
        if (line == null) throw new LineParseException("Empty line");

        String[] values = extract(line);
        if (values == null) throw new LineParseException("The line does not match the format " + format);

        String id = idIsBib(preferBib) ? values[bibIdx] : values[chipIdx];
        if (id.isEmpty()) throw new LineParseException("No " + (idIsBib(preferBib) ? "bib" : "chip"));

        Duration timestamp = timestamp(values, eventDate);
        if (timestamp.isNegative()) throw new LineParseException("The read is before the event date");

        return new ParsedRead(id, timestamp);
    }

    // The cleaned up value for each column, or null if the line doesn't match
    private String[] extract(String line) {
        String[] values = new String[columns.size()];
        if (linePattern != null) {
            Matcher m = linePattern.matcher(line);
            if (!m.matches()) return null;
            for (int i = 0; i < values.length; i++) {
                values[i] = clean(m.group(i + 1));
            }
        } else {
            String[] cells = split(line);
            if (cells.length < values.length) return null;
            for (int i = 0; i < values.length; i++) {
                values[i] = clean(cells[i]);
            }
        }
        return values;
    }

    /**
     * Split a line on its delimiter: the first of tab, comma, semicolon or
     * pipe found in the line, otherwise whitespace.
     *
     * @param line the line
     * @return the cells (not cleaned up)
     */
    static String[] split(String line) {
        String d = delimiter(line);
        if (d == null) return WHITESPACE.split(line.trim());
        return line.split(Pattern.quote(d), -1);
    }

    /**
     * @param line a line
     * @return the delimiter the line uses (tab, comma, semicolon or pipe) or
     * null if it has none of them (whitespace delimited)
     */
    static String delimiter(String line) {
        for (String d : new String[]{"\t", ",", ";", "|"}) {
            if (line.contains(d)) return d;
        }
        return null;
    }

    /**
     * Trim a value and strip any surrounding quotes (and a UTF-8 byte order mark).
     *
     * @param s the raw value
     * @return the cleaned up value, never null
     */
    static String clean(String s) {
        if (s == null) return "";
        s = s.replace("﻿", "").trim();
        while (s.length() >= 2
                && ((s.startsWith("\"") && s.endsWith("\"")) || (s.startsWith("'") && s.endsWith("'")))) {
            s = s.substring(1, s.length() - 1).trim();
        }
        return s;
    }

    private Duration timestamp(String[] values, LocalDate eventDate) throws LineParseException {
        // EpochMilli wins if we have one
        if (epochIdx >= 0 && !values[epochIdx].isEmpty()) {
            long millis;
            try {
                millis = Long.parseLong(values[epochIdx]);
            } catch (NumberFormatException e) {
                throw new LineParseException("Unable to parse the EpochMilli \"" + values[epochIdx] + "\"");
            }
            ZoneId zone = tzIdx >= 0 ? zoneFor(values[tzIdx]) : ZoneId.systemDefault();
            LocalDateTime ldt = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), zone);
            return Duration.between(eventDate.atStartOfDay(), ldt);
        }

        if (dateTimeIdx >= 0) {
            LocalDateTime ldt = parseDateTime(values[dateTimeIdx], columns.get(dateTimeIdx).formatter());
            if (ldt == null) throw new LineParseException("Unable to parse the date/time \"" + values[dateTimeIdx] + "\"");
            return Duration.between(eventDate.atStartOfDay(), ldt);
        }

        if (timeIdx < 0) throw new LineParseException("No time");
        String time = values[timeIdx];
        DateTimeFormatter timeFormatter = columns.get(timeIdx).formatter();

        Duration dayOffset = Duration.ZERO;
        if (dateIdx >= 0 && !values[dateIdx].isEmpty()) {
            LocalDate date = parseDate(values[dateIdx], columns.get(dateIdx).formatter());
            if (date == null) throw new LineParseException("Unable to parse the date \"" + values[dateIdx] + "\"");
            dayOffset = Duration.between(eventDate.atStartOfDay(), date.atStartOfDay());
        } else if (timeFormatter == null && !TIME_PATTERN.matcher(time).matches()) {
            // No date column, but the time may have a date on it
            LocalDateTime ldt = parseDateTime(time, null);
            if (ldt != null) return Duration.between(eventDate.atStartOfDay(), ldt);
        }

        Duration timeOfDay = parseTime(time, timeFormatter);
        if (timeOfDay == null) throw new LineParseException("Unable to parse the time \"" + time + "\"");
        return dayOffset.plus(timeOfDay);
    }

    /**
     * @param s a time of day (H:mm:ss[.fff] [AM/PM], hours past 24 are allowed)
     * @param formatter the pattern to use, or null for the default
     * @return the time since midnight, or null if it can't be parsed
     */
    static Duration parseTime(String s, DateTimeFormatter formatter) {
        if (s == null || s.isEmpty()) return null;
        if (formatter != null) {
            try {
                return Duration.ofNanos(LocalTime.parse(s, formatter).toNanoOfDay());
            } catch (DateTimeException e) {
                return null;
            }
        }
        Matcher m = TIME_PATTERN.matcher(s);
        if (!m.matches()) return null;
        long hours = Long.parseLong(m.group(1));
        long minutes = Long.parseLong(m.group(2));
        long seconds = Long.parseLong(m.group(3));
        if (minutes > 59 || seconds > 59) return null;
        long nanos = 0;
        if (m.group(4) != null) {
            String fraction = (m.group(4) + "000000000").substring(0, 9);
            nanos = Long.parseLong(fraction);
        }
        if (m.group(5) != null) {
            if (hours < 1 || hours > 12) return null;
            boolean pm = m.group(5).equalsIgnoreCase("p");
            if (hours == 12) hours = 0;
            if (pm) hours += 12;
        }
        return Duration.ofHours(hours).plusMinutes(minutes).plusSeconds(seconds).plusNanos(nanos);
    }

    /**
     * @param s a date (yyyy-MM-dd, M/d/yyyy, yyyy/M/d or M-d-yyyy)
     * @param formatter the pattern to use, or null to try the defaults
     * @return the date, or null if it can't be parsed
     */
    static LocalDate parseDate(String s, DateTimeFormatter formatter) {
        if (s == null || s.isEmpty()) return null;
        if (formatter != null) {
            try {
                return LocalDate.parse(s, formatter);
            } catch (DateTimeException e) {
                return null;
            }
        }
        for (DateTimeFormatter f : DATE_FORMATS) {
            try {
                return LocalDate.parse(s, f);
            } catch (DateTimeException e) {
                // try the next one
            }
        }
        return null;
    }

    /**
     * @param s a date and time separated by a space or a T, or an ISO date
     * time with an offset (converted to the local timezone)
     * @param formatter the pattern to use, or null to try the defaults
     * @return the date and time, or null if it can't be parsed
     */
    static LocalDateTime parseDateTime(String s, DateTimeFormatter formatter) {
        if (s == null || s.isEmpty()) return null;
        if (formatter != null) {
            try {
                return LocalDateTime.parse(s, formatter);
            } catch (DateTimeException e) {
                return null;
            }
        }
        try {
            return OffsetDateTime.parse(s).atZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime();
        } catch (DateTimeException e) {
            // not an ISO date time with an offset
        }
        Matcher m = DATE_TIME_SPLIT.matcher(s);
        if (!m.matches()) return null;
        LocalDate date = parseDate(m.group(1), null);
        Duration time = parseTime(m.group(2).trim(), null);
        if (date == null || time == null) return null;
        return date.atStartOfDay().plus(time);
    }

    /**
     * @param s a value
     * @return true if it looks like an EpochMilli (12 to 14 digits)
     */
    static boolean isEpoch(String s) {
        return s != null && EPOCH_PATTERN.matcher(s).matches();
    }

    // The timezone offset (e.g. -07:00 or America/Denver), or the local zone
    // if it is missing or invalid
    private static ZoneId zoneFor(String tz) {
        if (tz == null || tz.isBlank()) return ZoneId.systemDefault();
        return ZONE_CACHE.computeIfAbsent(tz, z -> {
            try {
                return ZoneId.of(z);
            } catch (DateTimeException e) {
                return ZoneId.systemDefault();
            }
        });
    }
}
