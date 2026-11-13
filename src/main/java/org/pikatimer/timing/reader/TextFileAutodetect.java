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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.pikatimer.timing.reader.TextLineParser.Field;

/**
 * Works out the format of a text file from its first few lines.
 *
 * If the file has a header line, the column names are used. Otherwise each
 * column is classified from its values. With no header the id column is
 * treated as a chip if most of its values are chips in the chip -> bib map,
 * otherwise as a bib.
 *
 * @author John Garner
 */
public final class TextFileAutodetect {

    private static final int SAMPLE_LINES = 25;

    // Plausible EpochMilli values: 2000-01-01 to 2100-01-01
    private static final long EPOCH_MIN = 946_684_800_000L;
    private static final long EPOCH_MAX = 4_102_444_800_000L;

    /**
     * What was detected.
     *
     * @param format the predefined format, or CUSTOM
     * @param customFormat the custom format string (for CUSTOM), or the
     * predefined format's equivalent
     * @param parser the parser to use, or null if nothing usable was found
     * @param hasHeader true if the first line is a header
     * @param description a short description for the UI
     */
    public record Result(TextFileFormat format, String customFormat, TextLineParser parser, boolean hasHeader, String description) {
        public boolean found() {
            return parser != null && parser.isValid();
        }
    }

    private enum Kind { EPOCH, DATETIME, DATE, TIME, ID, OTHER, EMPTY }

    private TextFileAutodetect() {
    }

    /**
     * @param file the file to look at
     * @param chip2bib the chips in the chip -> bib map (empty if no custom
     * map is in use)
     * @return what was detected
     */
    public static Result detect(File file, Set<String> chip2bib) {
        List<String> lines = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            String line;
            while (lines.size() < SAMPLE_LINES && (line = br.readLine()) != null) {
                line = line.replace("﻿", "").trim();
                if (!line.isEmpty()) lines.add(line);
            }
        } catch (IOException e) {
            return notFound("Unable to read the file: " + e.getMessage());
        }
        return detect(lines, chip2bib);
    }

    /**
     * @param lines the first few non-blank lines of the file
     * @param chip2bib the chips in the chip -> bib map (empty if no custom
     * map is in use)
     * @return what was detected
     */
    public static Result detect(List<String> lines, Set<String> chip2bib) {
        if (lines == null || lines.isEmpty()) return notFound("The file is empty");

        String delimiter = pickDelimiter(lines);
        List<String[]> rows = new ArrayList<>();
        for (String line : lines) rows.add(split(line, delimiter));

        boolean hasHeader = isHeader(rows.get(0));
        List<String[]> data = hasHeader ? rows.subList(1, rows.size()) : rows;
        int columnCount = 0;
        for (String[] r : rows) columnCount = Math.max(columnCount, r.length);

        // What the data in each column looks like
        Kind[] kinds = new Kind[columnCount];
        for (int c = 0; c < columnCount; c++) kinds[c] = classify(data, c);

        Field[] fields = new Field[columnCount];
        Arrays.fill(fields, Field.IGNORE);

        if (hasHeader) {
            String[] header = rows.get(0);
            for (int c = 0; c < header.length; c++) {
                Field f = fromHeaderName(header[c]);
                // A "Time" column that holds a date and time
                if (f == Field.TIME && kinds[c] == Kind.DATETIME) f = Field.DATETIME;
                if (f == Field.DATETIME && kinds[c] == Kind.TIME) f = Field.TIME;
                fields[c] = f;
            }
        }

        // Fill in a time from the data if the header didn't name one
        if (!has(fields, Field.EPOCH) && !has(fields, Field.DATETIME) && !has(fields, Field.TIME)) {
            fillTimeFromData(fields, kinds);
        }

        // Fill in an id from the data if the header didn't name one
        if (!has(fields, Field.BIB) && !has(fields, Field.CHIP)) {
            int idCol = pickIdColumn(data, kinds, fields);
            if (idCol >= 0) {
                fields[idCol] = looksLikeChips(data, idCol, chip2bib) ? Field.CHIP : Field.BIB;
            }
        }

        // The predefined formats ignore any extra columns at the end of the
        // line, so drop the trailing ignored columns before comparing
        List<Field> allFields = Arrays.asList(fields);
        int last = fields.length;
        while (last > 0 && fields[last - 1] == Field.IGNORE) last--;
        List<Field> fieldList = allFields.subList(0, last);

        String headerNote = hasHeader ? " (with a header line)" : " (no header line)";
        TextFileFormat predefined = TextFileFormat.matching(fieldList);
        if (predefined != null) {
            TextLineParser parser = predefined.parser();
            return new Result(predefined, customString(fieldList, delimiter), parser, hasHeader,
                    "Detected " + predefined + headerNote);
        }

        // A custom format has to match the whole line, so keep every column
        String custom = customString(allFields, delimiter);
        TextLineParser parser = TextLineParser.compileCustom(custom);
        if (!parser.isValid()) {
            return new Result(TextFileFormat.CUSTOM, custom, null, hasHeader,
                    "Unable to detect the format" + headerNote + ": " + parser.error());
        }
        return new Result(TextFileFormat.CUSTOM, custom, parser, hasHeader,
                "Detected custom format " + custom + headerNote);
    }

    private static Result notFound(String why) {
        return new Result(TextFileFormat.AUTODETECT, "", null, false, why);
    }

    // The delimiter that every sampled line uses (tab, comma, semicolon, pipe)
    // or null for whitespace
    private static String pickDelimiter(List<String> lines) {
        for (String d : new String[]{"\t", ",", ";", "|"}) {
            boolean all = true;
            for (String line : lines) {
                if (!line.contains(d)) {
                    all = false;
                    break;
                }
            }
            if (all) return d;
        }
        return null;
    }

    private static String[] split(String line, String delimiter) {
        String[] cells = delimiter == null
                ? line.trim().split("\\s+")
                : line.split(java.util.regex.Pattern.quote(delimiter), -1);
        for (int i = 0; i < cells.length; i++) cells[i] = TextLineParser.clean(cells[i]);
        return cells;
    }

    // A header has a cell with a letter in it and nothing that looks like a time
    private static boolean isHeader(String[] row) {
        boolean hasText = false;
        for (String cell : row) {
            Kind k = classify(cell);
            if (k == Kind.TIME || k == Kind.DATETIME || k == Kind.DATE || k == Kind.EPOCH) return false;
            if (cell.chars().anyMatch(Character::isLetter)) hasText = true;
        }
        return hasText;
    }

    private static Field fromHeaderName(String name) {
        String n = name.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
        return switch (n) {
            case "bib", "bibno", "bibnum", "bibnumber", "bibs" -> Field.BIB;
            case "chip", "chipid", "chipno", "chipnum", "chipnumber", "chipcode",
                 "tag", "tagid", "tagno", "epc", "rfid" -> Field.CHIP;
            case "date", "readdate", "racedate" -> Field.DATE;
            case "time", "tod", "timeofday", "readtime", "clocktime", "walltime" -> Field.TIME;
            case "datetime", "timestamp", "datetimestamp", "readtimestamp" -> Field.DATETIME;
            case "tz", "timezone", "tzoffset", "zone" -> Field.TZ;
            case "e", "epoch", "epochmilli", "epochmillis", "epochms" -> Field.EPOCH;
            default -> Field.IGNORE;
        };
    }

    private static Kind classify(String cell) {
        if (cell == null || cell.isEmpty()) return Kind.EMPTY;
        if (TextLineParser.isEpoch(cell)) {
            long v = Long.parseLong(cell);
            if (v >= EPOCH_MIN && v <= EPOCH_MAX) return Kind.EPOCH;
        }
        if (TextLineParser.parseTime(cell, null) != null) return Kind.TIME;
        if (TextLineParser.parseDate(cell, null) != null) return Kind.DATE;
        if (TextLineParser.parseDateTime(cell, null) != null) return Kind.DATETIME;
        if (cell.matches("[A-Za-z0-9_-]+")) return Kind.ID;
        return Kind.OTHER;
    }

    // The most common kind in a column (ignoring empty cells)
    private static Kind classify(List<String[]> data, int col) {
        int[] counts = new int[Kind.values().length];
        for (String[] row : data) {
            Kind k = col < row.length ? classify(row[col]) : Kind.EMPTY;
            counts[k.ordinal()]++;
        }
        Kind best = Kind.EMPTY;
        int bestCount = 0;
        for (Kind k : Kind.values()) {
            if (k != Kind.EMPTY && counts[k.ordinal()] > bestCount) {
                best = k;
                bestCount = counts[k.ordinal()];
            }
        }
        return best;
    }

    private static void fillTimeFromData(Field[] fields, Kind[] kinds) {
        int epoch = firstOf(kinds, fields, Kind.EPOCH);
        int dateTime = firstOf(kinds, fields, Kind.DATETIME);
        int time = firstOf(kinds, fields, Kind.TIME);
        int date = firstOf(kinds, fields, Kind.DATE);
        if (dateTime >= 0) {
            fields[dateTime] = Field.DATETIME;
        } else if (time >= 0) {
            fields[time] = Field.TIME;
            if (date >= 0 && !has(fields, Field.DATE)) fields[date] = Field.DATE;
        } else if (epoch >= 0) {
            fields[epoch] = Field.EPOCH;
        }
    }

    // The first unassigned column of a kind
    private static int firstOf(Kind[] kinds, Field[] fields, Kind kind) {
        for (int c = 0; c < kinds.length; c++) {
            if (kinds[c] == kind && fields[c] == Field.IGNORE) return c;
        }
        return -1;
    }

    // The unassigned id column with the most distinct values (so an antenna
    // or reader column is passed over in favor of the chips)
    private static int pickIdColumn(List<String[]> data, Kind[] kinds, Field[] fields) {
        int best = -1;
        int bestDistinct = 0;
        for (int c = 0; c < kinds.length; c++) {
            if (kinds[c] != Kind.ID || fields[c] != Field.IGNORE) continue;
            Set<String> distinct = new HashSet<>();
            for (String[] row : data) {
                if (c < row.length && !row[c].isEmpty()) distinct.add(row[c]);
            }
            if (distinct.size() > bestDistinct) {
                best = c;
                bestDistinct = distinct.size();
            }
        }
        return best;
    }

    // True if more than half of the values are chips in the chip -> bib map
    private static boolean looksLikeChips(List<String[]> data, int col, Set<String> chips) {
        if (chips == null || chips.isEmpty()) return false;
        int total = 0;
        int found = 0;
        for (String[] row : data) {
            if (col >= row.length || row[col].isEmpty() || "0".equals(row[col])) continue;
            total++;
            if (chips.contains(row[col])) found++;
        }
        return total > 0 && found * 2 > total;
    }

    private static boolean has(Field[] fields, Field f) {
        for (Field x : fields) if (x == f) return true;
        return false;
    }

    private static String customString(List<Field> fields, String delimiter) {
        String d = delimiter == null ? " " : delimiter.equals("\t") ? "\\t" : delimiter;
        StringBuilder sb = new StringBuilder();
        for (Field f : fields) {
            if (sb.length() > 0) sb.append(d);
            sb.append(f.code());
        }
        return sb.toString();
    }

    /**
     * @param map the chip -> bib map, may be null
     * @param inUse true if the custom chip -> bib map is turned on
     * @return the chips to check ids against when there is no header (empty
     * if no custom map is in use)
     */
    public static Set<String> chipsOf(Map<String, String> map, boolean inUse) {
        if (!inUse || map == null) return Set.of();
        return map.keySet();
    }
}
