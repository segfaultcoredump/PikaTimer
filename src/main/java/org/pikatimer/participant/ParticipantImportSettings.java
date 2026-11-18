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
package org.pikatimer.participant;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.h2.tools.Csv;
import org.pikatimer.race.Wave;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Everything the participant import wizard collects, plus the CSV helpers
 * shared by the wizard and the import task.
 */
class ParticipantImportSettings {
    private static final Logger logger = LoggerFactory.getLogger(ParticipantImportSettings.class);

    static final int SAMPLE_ROWS = 5;

    enum DuplicateHandling {
        SKIP("Skip"), MERGE("Merge into existing"), IMPORT("Import anyway");

        private final String label;
        DuplicateHandling(String label) { this.label = label; }
        @Override public String toString() { return label; }
    }

    enum WaveAssignment { BY_BIB, BY_COLUMN, FIXED }

    /**
     * Where a CSV column goes. STANDARD targets use the Participant.setAttributes()
     * key, CUSTOM targets use the CustomAttribute ID.
     */
    record ImportTarget(Kind kind, String key, Integer customID, String label) {
        enum Kind { IGNORE, STANDARD, CUSTOM, RACE, WAVE }

        static final ImportTarget IGNORE = new ImportTarget(Kind.IGNORE, null, null, "Ignore");
        static final ImportTarget RACE = new ImportTarget(Kind.RACE, null, null, "Race");
        static final ImportTarget WAVE = new ImportTarget(Kind.WAVE, null, null, "Wave");

        static ImportTarget standard(String key, String label) {
            return new ImportTarget(Kind.STANDARD, key, null, label);
        }
        static ImportTarget custom(CustomAttribute ca) {
            return new ImportTarget(Kind.CUSTOM, null, ca.getID(), ca.getName());
        }
        boolean isIgnore() { return kind == Kind.IGNORE; }
        @Override public String toString() { return label; }
    }

    /** The header, a few sample rows, and the row count of a CSV file */
    record CsvInfo(List<String> columns, List<String[]> sampleRows, int rowCount) { }

    // File
    File file;
    Charset charset = StandardCharsets.UTF_8;
    CsvInfo csvInfo;

    // Existing participants
    boolean replaceExisting = true;
    DuplicateHandling duplicateHandling = DuplicateHandling.SKIP;

    // Race / Wave assignment
    WaveAssignment waveAssignment = WaveAssignment.FIXED;
    Wave fixedWave;

    // CSV column index (0 based) -> target. Ignored columns are left out.
    final Map<Integer, ImportTarget> mapping = new LinkedHashMap<>();

    /**
     * UTF-8 if the whole file decodes cleanly, otherwise assume a Windows
     * (Cp1252) text file.
     */
    static Charset detectCharset(File f) throws IOException {
        CharsetDecoder utf8 = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        try (InputStream in = Files.newInputStream(f.toPath()); Reader r = new InputStreamReader(in, utf8)) {
            char[] buf = new char[8192];
            while (r.read(buf) >= 0) { /* decode only */ }
            return StandardCharsets.UTF_8;
        } catch (CharacterCodingException ex) {
            logger.debug("{} is not UTF-8: {}", f, ex.getMessage());
            return Charset.forName("Cp1252");
        }
    }

    /**
     * Read the header, the first few rows, and the row count.
     */
    static CsvInfo readCsvInfo(File f, Charset cs) throws SQLException {
        List<String> columns = new ArrayList<>();
        List<String[]> samples = new ArrayList<>();
        int rows = 0;
        Csv csv = new Csv();
        csv.setCaseSensitiveColumnNames(true); // show "Gender", not "GENDER"
        try (ResultSet rs = csv.read(f.getAbsolutePath(), null, cs.name())) {
            ResultSetMetaData meta = rs.getMetaData();
            int n = meta.getColumnCount();
            for (int i = 1; i <= n; i++) columns.add(meta.getColumnLabel(i));
            while (rs.next()) {
                if (samples.size() < SAMPLE_ROWS) {
                    String[] row = new String[n];
                    for (int i = 1; i <= n; i++) row[i - 1] = rs.getString(i);
                    samples.add(row);
                }
                rows++;
            }
        }
        return new CsvInfo(columns, samples, rows);
    }
}
