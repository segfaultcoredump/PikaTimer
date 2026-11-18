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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.RadioButton;
import javafx.scene.control.Separator;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.ToggleGroup;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;
import javafx.stage.Window;
import org.controlsfx.dialog.ProgressDialog;
import org.controlsfx.dialog.Wizard;
import org.controlsfx.dialog.WizardPane;
import org.pikatimer.PikaPreferences;
import org.pikatimer.participant.ParticipantImportSettings.CsvInfo;
import org.pikatimer.participant.ParticipantImportSettings.DuplicateHandling;
import org.pikatimer.participant.ParticipantImportSettings.ImportTarget;
import org.pikatimer.participant.ParticipantImportSettings.WaveAssignment;
import org.pikatimer.participant.ParticipantImportTask.ImportSummary;
import org.pikatimer.race.RaceDAO;
import org.pikatimer.race.Wave;
import org.pikatimer.util.WaveStringConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Participant CSV import wizard.
 *
 * Page 1: file, existing participant handling, race / wave assignment
 * Page 2: CSV column -> participant attribute mapping
 * Finish: runs a {@link ParticipantImportTask} behind a progress dialog
 */
public class ParticipantImportWizard {
    private static final Logger logger = LoggerFactory.getLogger(ParticipantImportWizard.class);

    private static final String ERROR_STYLE = "-fx-text-fill: #c62828;";
    private static final String WARNING_STYLE = "-fx-text-fill: #a35f00;";
    private static final String OK_STYLE = "-fx-text-fill: #2e7d32;";
    private static final String SECTION_STYLE = "-fx-font-weight: bold;";

    // Normalized CSV column name -> normalized target name, for common alternate column names
    private static final Map<String, String> SYNONYMS = Map.ofEntries(
            Map.entry("bibnumber", "bib"), Map.entry("bibno", "bib"), Map.entry("bibnum", "bib"),
            Map.entry("racenumber", "bib"),
            Map.entry("fname", "firstname"), Map.entry("first", "firstname"), Map.entry("givenname", "firstname"),
            Map.entry("mname", "middlename"), Map.entry("middle", "middlename"), Map.entry("middleinitial", "middlename"),
            Map.entry("mi", "middlename"),
            Map.entry("lname", "lastname"), Map.entry("last", "lastname"), Map.entry("surname", "lastname"),
            Map.entry("familyname", "lastname"),
            Map.entry("gender", "sex"), Map.entry("mf", "sex"),
            Map.entry("dob", "birthday"), Map.entry("dateofbirth", "birthday"), Map.entry("birthdate", "birthday"),
            Map.entry("bday", "birthday"),
            Map.entry("town", "city"),
            Map.entry("st", "state"), Map.entry("province", "state"), Map.entry("stateprovince", "state"),
            Map.entry("zip", "zipcode"), Map.entry("postalcode", "zipcode"), Map.entry("postcode", "zipcode"),
            Map.entry("postal", "zipcode"),
            Map.entry("countrycode", "country"),
            Map.entry("emailaddress", "email"), Map.entry("mail", "email"),
            Map.entry("notes", "note"), Map.entry("comment", "note"), Map.entry("comments", "note"),
            Map.entry("event", "race"), Map.entry("eventname", "race"), Map.entry("racename", "race"),
            Map.entry("distance", "race"), Map.entry("course", "race"),
            Map.entry("wavename", "wave"), Map.entry("startwave", "wave"), Map.entry("corral", "wave"));

    // Columns with these words are never matched on a partial name (e.g. "Age Group", "Emergency Contact Name")
    private static final List<String> NO_PARTIAL_MATCH = List.of("group", "division", "contact", "emergency", "parent", "guardian");

    private final Window owner;
    private final ParticipantImportSettings settings = new ParticipantImportSettings();

    // Page 1 state
    private final BooleanProperty fileValid = new SimpleBooleanProperty(false);
    private String checkedFileKey = "";
    private final TextField fileField = new TextField();
    private final Label fileStatusLabel = new Label();
    private final RadioButton replaceRadio = new RadioButton("Replace all existing participants");
    private final RadioButton addRadio = new RadioButton("Add to the existing participants");
    private final ComboBox<DuplicateHandling> duplicateCombo = new ComboBox<>();
    private final RadioButton byBibRadio = new RadioButton("By bib ranges from the race setup");
    private final RadioButton byColumnRadio = new RadioButton("From a Race and/or Wave column in the file");
    private final RadioButton fixedRadio = new RadioButton("Assign everyone to:");
    private final ComboBox<Wave> waveCombo = new ComboBox<>();
    private boolean existingParticipants;
    private boolean multipleWaves;

    // Page 2 state
    private final ObservableList<ColumnMapping> columnMappings = FXCollections.observableArrayList();
    private final ObservableList<ImportTarget> targets = FXCollections.observableArrayList();
    private final BooleanProperty mappingValid = new SimpleBooleanProperty(false);
    private final Label mappingStatusLabel = new Label();
    private String mappedFileKey = "";
    private WaveAssignment mappedWaveAssignment;

    /** One CSV column on the mapping page */
    static final class ColumnMapping {
        final int index;
        final String column;
        final String sample;
        final ObjectProperty<ImportTarget> target = new SimpleObjectProperty<>(ImportTarget.IGNORE);

        ColumnMapping(int index, String column, String sample) {
            this.index = index;
            this.column = column;
            this.sample = sample;
        }
    }

    public ParticipantImportWizard(Window owner) {
        this.owner = owner;
    }

    public void show() {
        Wizard wizard = new Wizard(owner, "Import Participants");
        WizardPane filePane = buildFilePane();
        WizardPane mappingPane = buildMappingPane();
        for (WizardPane p : List.of(filePane, mappingPane)) {
            p.getStylesheets().removeFirst(); // drop the ControlsFX wizard graphic, same as the RSU wizard
            p.setMinSize(620, 480); // same size for both pages so the dialog doesn't jump
        }
        wizard.setFlow(new Wizard.LinearFlow(filePane, mappingPane));
        wizard.getDialog().setResizable(true);

        wizard.showAndWait().ifPresent(result -> {
            if (result == ButtonType.FINISH) {
                collectOptions();
                collectMapping();
                runImport();
            }
        });
    }

    ////////////////////////////////////////////////////////////////////////
    //
    // Page 1: file and options
    //
    ////////////////////////////////////////////////////////////////////////

    private WizardPane buildFilePane() {
        VBox content = new VBox(8);
        content.setPadding(new Insets(10));

        // File
        Button browseButton = new Button("Browse…");
        browseButton.setOnAction(e -> chooseFile());
        fileField.setPromptText("CSV file with a header row");
        HBox.setHgrow(fileField, Priority.ALWAYS);
        HBox fileRow = new HBox(6, fileField, browseButton);
        fileRow.setAlignment(Pos.CENTER_LEFT);
        fileStatusLabel.setWrapText(true);
        fileField.textProperty().addListener((obs, o, n) -> checkFile(n));
        content.getChildren().addAll(section("Participant file"), fileRow, fileStatusLabel);

        // Existing participants
        int existing = ParticipantDAO.getInstance().listParticipants().size();
        existingParticipants = existing > 0;
        if (existingParticipants) {
            ToggleGroup existingGroup = new ToggleGroup();
            replaceRadio.setToggleGroup(existingGroup);
            addRadio.setToggleGroup(existingGroup);
            addRadio.setSelected(true);

            duplicateCombo.getItems().setAll(DuplicateHandling.values());
            duplicateCombo.setValue(DuplicateHandling.SKIP);
            Label dupeLabel = new Label("Duplicates (same name, age and sex):");
            HBox dupeRow = new HBox(6, dupeLabel, duplicateCombo);
            dupeRow.setAlignment(Pos.CENTER_LEFT);
            dupeRow.setPadding(new Insets(0, 0, 0, 24));
            dupeRow.disableProperty().bind(addRadio.selectedProperty().not());

            Label replaceWarning = new Label("All " + existing + " existing participants will be removed before the import.");
            replaceWarning.setStyle(WARNING_STYLE);
            replaceWarning.setPadding(new Insets(0, 0, 0, 24));
            replaceWarning.visibleProperty().bind(replaceRadio.selectedProperty());
            replaceWarning.managedProperty().bind(replaceRadio.selectedProperty());

            content.getChildren().addAll(new Separator(),
                    section("Existing participants (" + existing + ")"),
                    addRadio, dupeRow, replaceRadio, replaceWarning);
        }

        // Race / Wave assignment
        List<Wave> waves = new ArrayList<>(RaceDAO.getInstance().listWaves());
        multipleWaves = waves.size() > 1;
        if (multipleWaves) {
            ToggleGroup waveGroup = new ToggleGroup();
            byBibRadio.setToggleGroup(waveGroup);
            byColumnRadio.setToggleGroup(waveGroup);
            fixedRadio.setToggleGroup(waveGroup);

            boolean bibRanges = waves.stream().anyMatch(w -> w.getWaveAssignmentMethod() == org.pikatimer.race.WaveAssignment.BIB);
            if (bibRanges) {
                byBibRadio.setSelected(true);
            } else {
                byBibRadio.setText(byBibRadio.getText() + " (none set up)");
                byBibRadio.setDisable(true);
                byColumnRadio.setSelected(true);
            }

            waveCombo.setConverter(new WaveStringConverter());
            waveCombo.getItems().setAll(waves);
            waveCombo.setValue(waves.getFirst());
            waveCombo.disableProperty().bind(fixedRadio.selectedProperty().not());
            HBox fixedRow = new HBox(6, fixedRadio, waveCombo);
            fixedRow.setAlignment(Pos.CENTER_LEFT);

            content.getChildren().addAll(new Separator(), section("Race / Wave assignment"),
                    byBibRadio, byColumnRadio, fixedRow);
        }

        WizardPane pane = new WizardPane() {
            @Override
            public void onEnteringPage(Wizard wizard) {
                wizard.invalidProperty().unbind();
                wizard.invalidProperty().bind(fileValid.not());
            }

            @Override
            public void onExitingPage(Wizard wizard) {
                wizard.invalidProperty().unbind();
                collectOptions();
            }
        };
        pane.setHeaderText("Choose the participant file and import options");
        pane.setContent(content);
        return pane;
    }

    private static Label section(String text) {
        Label l = new Label(text);
        l.setStyle(SECTION_STYLE);
        return l;
    }

    private void chooseFile() {
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Open CSV File");
        File current = new File(fileField.getText().trim());
        if (!fileField.getText().isBlank() && current.getParentFile() != null && current.getParentFile().isDirectory()) {
            fileChooser.setInitialDirectory(current.getParentFile());
            fileChooser.setInitialFileName(current.getName());
        } else {
            fileChooser.setInitialDirectory(PikaPreferences.getInstance().getCWD());
        }
        fileChooser.getExtensionFilters().addAll(
                new FileChooser.ExtensionFilter("CSV/TXT Files", "*.csv", "*.txt"),
                new FileChooser.ExtensionFilter("All files", "*"));
        File file = fileChooser.showOpenDialog(fileField.getScene().getWindow());
        if (file != null) fileField.setText(file.getAbsolutePath());
    }

    private void checkFile(String path) {
        File f = new File(path == null ? "" : path.trim());
        String key = f.getAbsolutePath() + "|" + f.lastModified() + "|" + f.length();
        if (key.equals(checkedFileKey) && fileValid.get()) return;
        checkedFileKey = key;
        fileValid.set(false);

        String error = null;
        if (path == null || path.isBlank()) error = "";
        else if (!f.exists()) error = "File does not exist";
        else if (!f.isFile()) error = "The path entered is not a regular file";
        else if (!f.canRead()) error = "Unable to read the file";

        if (error == null) {
            try {
                Charset cs = ParticipantImportSettings.detectCharset(f);
                CsvInfo info = ParticipantImportSettings.readCsvInfo(f, cs);
                if (info.columns().isEmpty()) {
                    error = "No columns found. The first row must be a header row.";
                } else {
                    settings.file = f;
                    settings.charset = cs;
                    settings.csvInfo = info;
                    fileStatusLabel.setText("✓ " + info.rowCount() + (info.rowCount() == 1 ? " row, " : " rows, ")
                            + info.columns().size() + (info.columns().size() == 1 ? " column" : " columns")
                            + " (" + cs.displayName() + ")");
                    fileStatusLabel.setStyle(OK_STYLE);
                    fileValid.set(true);
                    return;
                }
            } catch (Exception ex) {
                logger.debug("Unable to read {} as CSV", f, ex);
                error = "Unable to read this file as CSV: " + ex.getMessage();
            }
        }
        settings.file = null;
        settings.csvInfo = null;
        fileStatusLabel.setText(error);
        fileStatusLabel.setStyle(ERROR_STYLE);
    }

    /** Copy the page 1 controls into the settings */
    private void collectOptions() {
        settings.replaceExisting = !existingParticipants || replaceRadio.isSelected();
        settings.duplicateHandling = duplicateCombo.getValue() == null ? DuplicateHandling.SKIP : duplicateCombo.getValue();

        if (!multipleWaves) {
            settings.waveAssignment = WaveAssignment.FIXED;
            ObservableList<Wave> waves = RaceDAO.getInstance().listWaves();
            settings.fixedWave = waves.isEmpty() ? null : waves.getFirst();
        } else if (byBibRadio.isSelected()) {
            settings.waveAssignment = WaveAssignment.BY_BIB;
        } else if (byColumnRadio.isSelected()) {
            settings.waveAssignment = WaveAssignment.BY_COLUMN;
        } else {
            settings.waveAssignment = WaveAssignment.FIXED;
            settings.fixedWave = waveCombo.getValue();
        }
    }

    ////////////////////////////////////////////////////////////////////////
    //
    // Page 2: column mapping
    //
    ////////////////////////////////////////////////////////////////////////

    private WizardPane buildMappingPane() {
        TableView<ColumnMapping> table = new TableView<>(columnMappings);
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_LAST_COLUMN);
        table.setPlaceholder(new Label("No columns"));
        VBox.setVgrow(table, Priority.ALWAYS);
        table.setPrefHeight(300);

        TableColumn<ColumnMapping, String> columnCol = new TableColumn<>("CSV Column");
        columnCol.setCellValueFactory(cd -> new SimpleStringProperty(cd.getValue().column));
        columnCol.setPrefWidth(150);
        columnCol.setStyle("-fx-alignment: CENTER-LEFT;");

        TableColumn<ColumnMapping, String> sampleCol = new TableColumn<>("Sample Values");
        sampleCol.setCellValueFactory(cd -> new SimpleStringProperty(cd.getValue().sample));
        sampleCol.setPrefWidth(220);
        sampleCol.setCellFactory(tc -> new TableCell<>() {
            @Override
            protected void updateItem(String s, boolean empty) {
                super.updateItem(s, empty);
                setText(empty ? null : s);
                setTooltip(empty || s == null || s.isEmpty() ? null : new Tooltip(s));
                setStyle("-fx-opacity: 0.7; -fx-alignment: CENTER-LEFT;");
            }
        });

        TableColumn<ColumnMapping, ImportTarget> targetCol = new TableColumn<>("Import As");
        targetCol.setCellValueFactory(cd -> cd.getValue().target);
        targetCol.setPrefWidth(200);
        targetCol.setCellFactory(tc -> new TableCell<>() {
            private final ComboBox<ImportTarget> combo = new ComboBox<>(targets);
            {
                combo.setMaxWidth(Double.MAX_VALUE);
                combo.setOnAction(e -> {
                    ColumnMapping m = getTableRow() == null ? null : getTableRow().getItem();
                    if (m != null && combo.getValue() != null) m.target.set(combo.getValue());
                });
            }

            @Override
            protected void updateItem(ImportTarget t, boolean empty) {
                super.updateItem(t, empty);
                if (empty) {
                    setGraphic(null);
                } else {
                    combo.setValue(t);
                    setGraphic(combo);
                }
            }
        });

        table.getColumns().add(columnCol);
        table.getColumns().add(sampleCol);
        table.getColumns().add(targetCol);

        mappingStatusLabel.setWrapText(true);
        mappingStatusLabel.setMinHeight(Region.USE_PREF_SIZE);

        VBox content = new VBox(8, table, mappingStatusLabel);
        content.setPadding(new Insets(10));

        WizardPane pane = new WizardPane() {
            @Override
            public void onEnteringPage(Wizard wizard) {
                rebuildMappingIfNeeded();
                wizard.invalidProperty().unbind();
                wizard.invalidProperty().bind(mappingValid.not());
            }

            @Override
            public void onExitingPage(Wizard wizard) {
                wizard.invalidProperty().unbind();
                collectMapping();
            }
        };
        pane.setHeaderText("Match the CSV columns to participant attributes");
        pane.setContent(content);
        return pane;
    }

    /**
     * Rebuild the mapping table when the file or the race/wave method changed
     * since it was last built. Otherwise keep the user's edits.
     */
    private void rebuildMappingIfNeeded() {
        if (settings.csvInfo == null) return;
        if (checkedFileKey.equals(mappedFileKey) && settings.waveAssignment == mappedWaveAssignment) return;

        Map<Integer, ImportTarget> previous = new HashMap<>();
        boolean sameFile = checkedFileKey.equals(mappedFileKey);
        if (sameFile) columnMappings.forEach(m -> previous.put(m.index, m.target.get()));

        mappedFileKey = checkedFileKey;
        mappedWaveAssignment = settings.waveAssignment;

        // Available targets
        List<ImportTarget> t = new ArrayList<>();
        t.add(ImportTarget.IGNORE);
        Participant.getAvailableAttributes().forEach((k, v) -> t.add(ImportTarget.standard(k, v)));
        ParticipantDAO.getInstance().getCustomAttributes().forEach(ca -> t.add(ImportTarget.custom(ca)));
        if (settings.waveAssignment == WaveAssignment.BY_COLUMN) {
            int races = RaceDAO.getInstance().listRaces().size();
            if (races > 1) t.add(ImportTarget.RACE);
            if (RaceDAO.getInstance().listWaves().size() > races) t.add(ImportTarget.WAVE);
        }
        targets.setAll(t);

        // Rows
        CsvInfo info = settings.csvInfo;
        List<ColumnMapping> rows = new ArrayList<>();
        for (int i = 0; i < info.columns().size(); i++) {
            final int col = i;
            String sample = info.sampleRows().stream()
                    .map(r -> col < r.length ? r[col] : null)
                    .filter(s -> s != null && !s.isBlank())
                    .map(String::trim)
                    .distinct()
                    .limit(3)
                    .collect(Collectors.joining("  ·  "));
            rows.add(new ColumnMapping(i, info.columns().get(i), sample));
        }

        if (sameFile) {
            // Only the race/wave method changed: keep what still applies
            rows.forEach(r -> {
                ImportTarget p = previous.get(r.index);
                if (p != null && t.contains(p)) r.target.set(p);
            });
            Set<ImportTarget> used = rows.stream().map(r -> r.target.get()).collect(Collectors.toSet());
            autoMatch(rows.stream().filter(r -> r.target.get().isIgnore()).toList(),
                    t.stream().filter(x -> !used.contains(x)).toList());
        } else {
            autoMatch(rows, t);
        }

        rows.forEach(r -> r.target.addListener((obs, o, n) -> validateMapping()));
        columnMappings.setAll(rows);
        validateMapping();
    }

    private static String normalize(String s) {
        return s == null ? "" : s.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
    }

    /**
     * Match CSV columns to targets: exact (normalized) names and synonyms
     * first, then a partial name match, but only when it is unambiguous.
     * A target is never auto-assigned to two columns.
     */
    static void autoMatch(List<ColumnMapping> rows, List<ImportTarget> available) {
        Map<ImportTarget, Set<String>> names = new HashMap<>();
        for (ImportTarget t : available) {
            if (t.isIgnore()) continue;
            Set<String> n = new LinkedHashSet<>();
            n.add(normalize(t.label()));
            if (t.key() != null) n.add(normalize(t.key()));
            n.remove("");
            names.put(t, n);
        }
        Set<ImportTarget> used = new HashSet<>();

        // Pass 1: exact names and synonyms
        for (ColumnMapping r : rows) {
            String col = normalize(r.column);
            if (col.isEmpty()) continue;
            String syn = SYNONYMS.get(col);
            for (ImportTarget t : available) {
                if (used.contains(t) || !names.containsKey(t)) continue;
                Set<String> n = names.get(t);
                if (n.contains(col) || (syn != null && n.contains(syn))) {
                    r.target.set(t);
                    used.add(t);
                    break;
                }
            }
        }

        // Pass 2: partial names, unambiguous only. The column either contains
        // a target name ("Participant First Name") or abbreviates one ("Zip").
        names.values().forEach(n -> {
            if (n.contains("sex")) n.add("gender");
        });
        for (ColumnMapping r : rows) {
            if (!r.target.get().isIgnore()) continue;
            String col = normalize(r.column);
            if (col.length() < 3 || NO_PARTIAL_MATCH.stream().anyMatch(col::contains)) continue;
            List<ImportTarget> candidates = new ArrayList<>();
            for (ImportTarget t : available) {
                if (!names.containsKey(t)) continue;
                boolean partial = names.get(t).stream()
                        .anyMatch(n -> n.length() >= 3 && (col.contains(n) || n.startsWith(col)));
                if (partial) candidates.add(t);
            }
            if (candidates.size() == 1 && !used.contains(candidates.getFirst())) {
                r.target.set(candidates.getFirst());
                used.add(candidates.getFirst());
            }
        }
    }

    private void validateMapping() {
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        Map<ImportTarget, Long> counts = columnMappings.stream()
                .map(m -> m.target.get())
                .filter(t -> t != null && !t.isIgnore())
                .collect(Collectors.groupingBy(t -> t, Collectors.counting()));

        if (counts.isEmpty()) errors.add("Choose an attribute for at least one column.");
        counts.forEach((t, n) -> {
            if (n > 1) errors.add("“" + t.label() + "” is selected for " + n + " columns.");
        });

        Set<String> keys = counts.keySet().stream().map(ImportTarget::key).filter(Objects::nonNull).collect(Collectors.toSet());
        if (!counts.isEmpty()) {
            if (!keys.contains("bib")) {
                warnings.add(settings.waveAssignment == WaveAssignment.BY_BIB
                        ? "No Bib column: participants can't be assigned to a race by bib range."
                        : "No Bib column: participants will be imported without bib numbers.");
            }
            if (!keys.contains("first") && !keys.contains("last")) warnings.add("No First or Last Name column.");
            if (settings.waveAssignment == WaveAssignment.BY_COLUMN
                    && !counts.containsKey(ImportTarget.RACE) && !counts.containsKey(ImportTarget.WAVE)) {
                warnings.add("No Race or Wave column: participants won't be assigned to a race.");
            }
        }

        mappingValid.set(errors.isEmpty());
        if (!errors.isEmpty()) {
            mappingStatusLabel.setText(String.join("\n", errors));
            mappingStatusLabel.setStyle(ERROR_STYLE);
        } else if (!warnings.isEmpty()) {
            mappingStatusLabel.setText(String.join("\n", warnings));
            mappingStatusLabel.setStyle(WARNING_STYLE);
        } else {
            int rows = settings.csvInfo == null ? 0 : settings.csvInfo.rowCount();
            mappingStatusLabel.setText("Ready to import " + rows + (rows == 1 ? " row." : " rows."));
            mappingStatusLabel.setStyle(OK_STYLE);
        }
    }

    /** Copy the mapping table into the settings */
    private void collectMapping() {
        settings.mapping.clear();
        columnMappings.forEach(m -> {
            ImportTarget t = m.target.get();
            if (t != null && !t.isIgnore()) settings.mapping.put(m.index, t);
        });
    }

    ////////////////////////////////////////////////////////////////////////
    //
    // Finish: import
    //
    ////////////////////////////////////////////////////////////////////////

    private void runImport() {
        if (settings.file == null || settings.mapping.isEmpty()) return;
        logger.debug("Importing {} ({}): replace={} dupes={} waves={} fixedWave={} mapping={}",
                settings.file, settings.charset, settings.replaceExisting, settings.duplicateHandling,
                settings.waveAssignment, settings.fixedWave == null ? null : settings.fixedWave.getID(), settings.mapping);

        ParticipantImportTask task = new ParticipantImportTask(settings);

        ProgressDialog progress = new ProgressDialog(task);
        if (owner != null) progress.initOwner(owner);
        progress.setTitle("Import Participants");
        progress.setHeaderText("Importing " + settings.file.getName());

        task.setOnSucceeded(e -> showSummary(task.getValue()));
        task.setOnFailed(e -> {
            Throwable ex = task.getException();
            logger.error("Participant import failed", ex);
            Alert alert = new Alert(Alert.AlertType.ERROR);
            if (owner != null) alert.initOwner(owner);
            alert.setTitle("Import Participants");
            alert.setHeaderText("The import failed");
            alert.setContentText((ex == null || ex.getMessage() == null ? "Unknown error" : ex.getMessage())
                    + "\n\nParticipants imported before the error were kept.");
            alert.show();
        });

        Thread t = new Thread(task, "participant-import");
        t.setDaemon(true);
        t.start();
        progress.show();
    }

    private void showSummary(ImportSummary s) {
        List<String> lines = new ArrayList<>();
        lines.add("Added " + s.added() + (s.added() == 1 ? " participant." : " participants."));
        if (s.merged() > 0) lines.add("Merged " + s.merged() + " into existing participants.");
        if (s.skipped() > 0) lines.add("Skipped " + s.skipped() + (s.skipped() == 1 ? " duplicate." : " duplicates."));
        if (s.bibsRenamed() > 0) lines.add(s.bibsRenamed() + (s.bibsRenamed() == 1 ? " bib was" : " bibs were")
                + " already in use and now start with “" + ParticipantImportTask.DUPE_BIB_PREFIX.trim() + "”.");
        if (s.unassigned() > 0) lines.add(s.unassigned() + (s.unassigned() == 1 ? " participant is" : " participants are")
                + " not assigned to a race.");

        Alert alert = new Alert(s.bibsRenamed() > 0 || s.unassigned() > 0 ? Alert.AlertType.WARNING : Alert.AlertType.INFORMATION);
        if (owner != null) alert.initOwner(owner);
        alert.setTitle("Import Participants");
        alert.setHeaderText("Imported " + s.rows() + (s.rows() == 1 ? " row" : " rows") + " from " + settings.file.getName());
        alert.setContentText(String.join("\n", lines));
        alert.show();
    }
}
