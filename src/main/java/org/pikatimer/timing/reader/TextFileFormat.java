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

import java.util.List;
import org.pikatimer.timing.reader.TextLineParser.Field;

/**
 * The file formats the Text File reader understands.
 *
 * The predefined formats match the basic output formats of PikaDownloader
 * and accept comma, tab, semicolon or pipe delimited files.
 *
 * @author John Garner
 */
public enum TextFileFormat {

    AUTODETECT("Autodetect", null),
    BIB_TIME("Bib,Time", List.of(Field.BIB, Field.TIME)),
    CHIP_TIME("Chip,Time", List.of(Field.CHIP, Field.TIME)),
    BIB_DATE_TIME("Bib,Date,Time", List.of(Field.BIB, Field.DATE, Field.TIME)),
    CHIP_DATE_TIME("Chip,Date,Time", List.of(Field.CHIP, Field.DATE, Field.TIME)),
    CUSTOM("Custom", null);

    private final String label;
    private final List<Field> columns;

    TextFileFormat(String label, List<Field> columns) {
        this.label = label;
        this.columns = columns;
    }

    /**
     * @return the columns for a predefined format, or null for AUTODETECT and CUSTOM
     */
    public List<Field> columns() {
        return columns;
    }

    /**
     * @return true for the predefined formats (not AUTODETECT or CUSTOM)
     */
    public boolean isPredefined() {
        return columns != null;
    }

    /**
     * @return the parser for a predefined format, or null for AUTODETECT and CUSTOM
     */
    public TextLineParser parser() {
        return columns == null ? null : TextLineParser.ofColumns(columns);
    }

    /**
     * @return the custom format string for a predefined format (e.g. "$B,$T"),
     * or "" for AUTODETECT and CUSTOM
     */
    public String customEquivalent() {
        if (columns == null) return "";
        StringBuilder sb = new StringBuilder();
        for (Field f : columns) {
            if (sb.length() > 0) sb.append(',');
            sb.append(f.code());
        }
        return sb.toString();
    }

    /**
     * @param columns a list of columns
     * @return the predefined format with exactly those columns, or null
     */
    public static TextFileFormat matching(List<Field> columns) {
        for (TextFileFormat f : values()) {
            if (f.columns != null && f.columns.equals(columns)) return f;
        }
        return null;
    }

    /**
     * @param name the enum name (as saved in the input's attributes)
     * @return the format, or AUTODETECT if the name is null or unknown
     */
    public static TextFileFormat fromName(String name) {
        if (name == null) return AUTODETECT;
        try {
            return valueOf(name);
        } catch (IllegalArgumentException e) {
            return AUTODETECT;
        }
    }

    @Override
    public String toString() {
        return label;
    }
}
