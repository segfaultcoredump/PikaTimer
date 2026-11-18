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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javafx.application.Platform;
import javafx.concurrent.Task;
import org.h2.tools.Csv;
import org.pikatimer.participant.ParticipantImportSettings.DuplicateHandling;
import org.pikatimer.participant.ParticipantImportSettings.ImportTarget;
import org.pikatimer.participant.ParticipantImportSettings.WaveAssignment;
import org.pikatimer.race.Race;
import org.pikatimer.race.RaceDAO;
import org.pikatimer.race.Wave;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Imports participants from a CSV file using the settings collected by the
 * {@link ParticipantImportWizard}.
 */
class ParticipantImportTask extends Task<ParticipantImportTask.ImportSummary> {
    private static final Logger logger = LoggerFactory.getLogger(ParticipantImportTask.class);

    static final String DUPE_BIB_PREFIX = "Dupe ";

    record ImportSummary(int rows, int added, int merged, int skipped, int bibsRenamed, int unassigned) { }

    private final ParticipantImportSettings settings;

    ParticipantImportTask(ParticipantImportSettings settings) {
        this.settings = settings;
    }

    private static String norm(String s) {
        return s == null ? "" : s.trim().toLowerCase(Locale.ROOT);
    }

    private static String dupeKey(Participant p) {
        return norm(p.getFirstName() + p.getLastName() + p.getAge() + p.getSex());
    }

    @Override
    protected ImportSummary call() throws Exception {
        ParticipantDAO participantDAO = ParticipantDAO.getInstance();
        int rowCount = settings.csvInfo == null ? 0 : settings.csvInfo.rowCount();
        int row = 0, added = 0, merged = 0, skipped = 0, bibsRenamed = 0, unassigned = 0;
        updateProgress(0, Math.max(rowCount, 1));

        // Race / Wave lookups for assignment from a column
        Map<String, Race> raceMap = new HashMap<>();
        Map<String, Wave> waveMap = new HashMap<>();
        if (settings.waveAssignment == WaveAssignment.BY_COLUMN) {
            RaceDAO.getInstance().listRaces().forEach(r -> raceMap.putIfAbsent(norm(r.getRaceName()), r));
            RaceDAO.getInstance().listWaves().forEach(w -> waveMap.putIfAbsent(norm(w.getWaveName()), w));
        }

        // Existing participants: either clear them or set up the duplicate check
        boolean dupeCheck = false;
        boolean mergeDupes = false;
        Map<String, Participant> existingMap = new HashMap<>();
        if (!participantDAO.listParticipants().isEmpty()) {
            if (settings.replaceExisting) {
                updateMessage("Clearing the existing participants...");
                participantDAO.blockingClearAll();
            } else if (settings.duplicateHandling != DuplicateHandling.IMPORT) {
                dupeCheck = true;
                mergeDupes = settings.duplicateHandling == DuplicateHandling.MERGE;
                new ArrayList<>(participantDAO.listParticipants()).forEach(p -> existingMap.put(dupeKey(p), p));
                logger.debug("Duplicate check against {} existing participants (merge: {})", existingMap.size(), mergeDupes);
            }
        }

        try (ResultSet rs = new Csv().read(settings.file.getAbsolutePath(), null, settings.charset.name())) {
            ResultSetMetaData meta = rs.getMetaData();
            int columns = meta.getColumnCount();

            while (rs.next()) {
                if (isCancelled()) break;
                row++;

                Map<String, String> attributes = new HashMap<>();
                Map<Integer, String> customAttributes = new HashMap<>();
                String pendingWave = "";
                String pendingRace = "";
                for (int i = 0; i < columns; i++) {
                    ImportTarget target = settings.mapping.get(i);
                    String value = rs.getString(i + 1);
                    if (target == null || target.isIgnore() || value == null || value.isEmpty()) continue;
                    switch (target.kind()) {
                        case STANDARD -> attributes.put(target.key(), value);
                        case CUSTOM -> customAttributes.put(target.customID(), value);
                        case RACE -> pendingRace = value;
                        case WAVE -> pendingWave = value;
                        default -> { }
                    }
                }

                Participant p = new Participant(attributes);
                p.setCustomAttributes(customAttributes);

                // Duplicate people
                if (dupeCheck && existingMap.containsKey(dupeKey(p))) {
                    Participant existing = existingMap.get(dupeKey(p));
                    if (mergeDupes) {
                        String newBib = attributes.get("bib");
                        if (newBib != null && !newBib.equals(existing.getBib())
                                && participantDAO.getParticipantByBib(newBib) != null) {
                            attributes.put("bib", DUPE_BIB_PREFIX + newBib);
                            bibsRenamed++;
                        }
                        // The participant may be bound to the UI, so update it on the FX thread
                        Platform.runLater(() -> {
                            existing.setAttributes(attributes);
                            customAttributes.forEach(existing::setCustomAttribute);
                            participantDAO.updateParticipant(existing);
                        });
                        merged++;
                        updateMessage("Merging " + existing.getFirstName() + " " + existing.getLastName());
                    } else {
                        skipped++;
                        updateMessage("Skipping " + p.getFirstName() + " " + p.getLastName());
                    }
                    updateProgress(row, rowCount);
                    continue;
                }

                // Duplicate bibs
                String bib = p.getBib();
                if (participantDAO.getParticipantByBib(bib) != null) {
                    p.setBib(DUPE_BIB_PREFIX + bib);
                    bibsRenamed++;
                }

                // Race / Wave assignment
                switch (settings.waveAssignment) {
                    case BY_BIB -> {
                        if (bib != null && !bib.isBlank()) p.setWaves(participantDAO.getWaveByBib(bib));
                    }
                    case BY_COLUMN -> {
                        Wave w = findWave(pendingRace, pendingWave, raceMap, waveMap);
                        if (w != null) p.setWaves(w);
                    }
                    case FIXED -> {
                        if (settings.fixedWave != null) p.addWave(settings.fixedWave);
                    }
                }
                if (p.wavesObservableList().isEmpty()) unassigned++;

                participantDAO.addParticipant(p);
                added++;

                updateProgress(row, rowCount);
                updateMessage("Adding " + p.getFirstName() + " " + p.getLastName());
            }
        }

        ImportSummary summary = new ImportSummary(row, added, merged, skipped, bibsRenamed, unassigned);
        logger.info("Participant import from {}: {}", settings.file, summary);
        updateProgress(1, 1);
        updateMessage("Done");
        return summary;
    }

    /**
     * Find the wave for a race and/or wave name from the CSV. Wave names like
     * "Wave 1" repeat across races, so a known race wins.
     */
    static Wave findWave(String raceName, String waveName, Map<String, Race> raceMap, Map<String, Wave> waveMap) {
        Race race = raceMap.get(norm(raceName));
        String wn = norm(waveName);
        if (race != null) {
            List<Wave> waves = race.getWaves();
            if (waves == null || waves.isEmpty()) return null;
            if (!wn.isEmpty()) {
                for (Wave w : waves) {
                    if (norm(w.getWaveName()).equals(wn)) return w;
                }
            }
            return waves.getFirst();
        }
        if (!wn.isEmpty()) {
            if (waveMap.containsKey(wn)) return waveMap.get(wn);
            // A race name in the wave column
            Race r = raceMap.get(wn);
            if (r != null && r.getWaves() != null && !r.getWaves().isEmpty()) return r.getWaves().getFirst();
        }
        return null;
    }
}
