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

import java.io.File;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.geometry.Pos;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonBar;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.scene.layout.Priority;
import javafx.util.Duration;
import org.pikatimer.timing.Bib2ChipMap;
import org.pikatimer.timing.RawTimeData;
import org.pikatimer.timing.TimingDAO;
import org.pikatimer.timing.TimingListener;
import org.pikatimer.timing.reader.TextLineParser.LineParseException;
import org.pikatimer.timing.reader.TextLineParser.ParsedRead;
import org.pikatimer.util.DurationFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Imports reads from a text file (and optionally tails it for new reads).
 *
 * The format is picked from a drop down: a basic "Bib,Time" style format,
 * a custom format string (the same escape codes as the PikaDownloader custom
 * output format), or Autodetect, which works it out from the header line or
 * the data. The first line of the file is skipped if it can't be parsed, so
 * files with or without a header line both work.
 *
 * chipIsBib() follows the format: a format with only a Bib column is read as
 * bibs, anything with a Chip column goes through the chip -> bib map (unless
 * the "Use Bib column" option is on for a file with both).
 *
 * @author John Garner
 */
public class PikaTextFileReader extends TailingReader {

    private static final Logger logger = LoggerFactory.getLogger(PikaTextFileReader.class);

    private static final String FORMAT_ATTRIBUTE = "TextFileReader:format";
    private static final String CUSTOM_FORMAT_ATTRIBUTE = "TextFileReader:customFormat";
    private static final String PREFER_BIB_ATTRIBUTE = "TextFileReader:preferBib";

    // How many lines into the file Autodetect keeps trying if it has not
    // found a usable format yet
    private static final int REDETECT_LINES = 25;

    private static final String CUSTOM_FORMAT_HELP = """
            Codes start with $ or % and may be wrapped in {}:
              $B  Bib            $C  Chip
              $D  Date           $T  Time of day
              $DT Date and Time  $E  EpochMilli
              $Z  Timezone (used with EpochMilli)
              $A, $R, $S, $X  Antenna, Reader, RSSI, Skip (ignored)
            Date and time codes take an optional pattern, e.g. %{D:MM/dd/yyyy}
            Use \\t for a tab and \\$ for a literal $.
            Example: $C,"$DT",%Z,%{EpochMilli}""";

    // The settings (changed on the FX thread, read on the reading threads)
    private volatile TextFileFormat format = TextFileFormat.AUTODETECT;
    private volatile String customFormat = "";
    private volatile boolean preferBib = false;

    // The parser in use. For Autodetect it is replaced each time the file is
    // read from the top.
    private volatile TextLineParser parser;
    private volatile TextFileAutodetect.Result detected;

    // Per pass over the file. Lines are processed one at a time, either by
    // the readOnce task or by the Tailer.
    private volatile int lineNumber = 0;
    private volatile int parsedCount = 0;
    private volatile int skippedCount = 0;
    private volatile String lastStatus = "";
    private final AtomicBoolean statusUpdatePending = new AtomicBoolean(false);

    // UI
    private HBox formatHBox;
    private ComboBox<TextFileFormat> formatComboBox;
    private TextField customFormatTextField;
    private CheckBox useBibCheckBox;
    private Button useAsCustomButton;
    private Label formatLabel;
    private boolean updatingControls = false;

    public PikaTextFileReader() {
        super();
    }

    @Override
    public void setTimingListener(TimingListener t) {
        super.setTimingListener(t);

        format = TextFileFormat.fromName(timingListener.getAttribute(FORMAT_ATTRIBUTE));
        String custom = timingListener.getAttribute(CUSTOM_FORMAT_ATTRIBUTE);
        customFormat = custom == null ? "" : custom;
        preferBib = Boolean.parseBoolean(timingListener.getAttribute(PREFER_BIB_ATTRIBUTE));

        logger.debug("PikaTextFileReader: format {} custom \"{}\" preferBib {}", format, customFormat, preferBib);
        parser = buildParser(format, customFormat, sourceFile);
    }

    // The parser for a format. For Autodetect this reads the start of the file.
    private TextLineParser buildParser(TextFileFormat f, String custom, File file) {
        return switch (f) {
            case AUTODETECT -> {
                TextFileAutodetect.Result r = detect(file);
                detected = r;
                yield r == null ? null : r.parser();
            }
            case CUSTOM -> TextLineParser.compileCustom(custom);
            default -> f.parser();
        };
    }

    private TextFileAutodetect.Result detect(File file) {
        if (file == null || !file.isFile() || !file.canRead()) return null;
        Set<String> chips;
        try {
            Bib2ChipMap map = TimingDAO.getInstance().getBib2ChipMap();
            chips = TextFileAutodetect.chipsOf(map.getChip2BibMap(), Boolean.TRUE.equals(map.getUseCustomMap()));
        } catch (Exception e) {
            logger.debug("Unable to get the chip -> bib map for autodetect", e);
            chips = Set.of();
        }
        TextFileAutodetect.Result r = TextFileAutodetect.detect(file, chips);
        logger.debug("PikaTextFileReader: autodetect of {}: {}", file, r.description());
        return r;
    }

    @Override
    protected void beforeFileRead() {
        lineNumber = 0;
        parsedCount = 0;
        skippedCount = 0;
        lastStatus = "";
        if (format == TextFileFormat.AUTODETECT) {
            parser = buildParser(TextFileFormat.AUTODETECT, customFormat, sourceFile);
            Platform.runLater(this::updateFormatControls);
        }
    }

    @Override
    public void process(String line) {
        int n = ++lineNumber;

        TextLineParser p = parser;
        // Autodetect on a file that was empty (or only had a header): try
        // again now that there is more in it
        if ((p == null || !p.isValid()) && format == TextFileFormat.AUTODETECT && n <= REDETECT_LINES) {
            p = buildParser(TextFileFormat.AUTODETECT, customFormat, sourceFile);
            parser = p;
            Platform.runLater(this::updateFormatControls);
        }
        if (p == null || !p.isValid()) {
            skippedCount++;
            lastStatus = "Unable to read line " + n + ": " + (p == null ? "the file format is unknown" : p.error());
            updateStatus();
            return;
        }

        try {
            ParsedRead read = p.parse(line, timingListener.getEventDate(), preferBib);
            RawTimeData rawTime = new RawTimeData();
            rawTime.setChip(read.id());
            rawTime.setTimestampLong(read.timestamp().toNanos());
            timingListener.processRead(rawTime);
            parsedCount++;
            lastStatus = "Last read: " + read.id() + " at " + DurationFormatter.durationToString(read.timestamp(), 3);
        } catch (LineParseException e) {
            if (n == 1) {
                // Most likely the header line
                logger.debug("Skipping the first line (header?): {}", line);
                return;
            }
            skippedCount++;
            lastStatus = "Skipped line " + n + " (" + e.getMessage() + "): " + line;
            logger.debug(lastStatus);
        }
        updateStatus();
    }

    // At most one status update is ever waiting on the FX thread
    private void updateStatus() {
        if (statusUpdatePending.compareAndSet(false, true)) {
            Platform.runLater(() -> {
                statusUpdatePending.set(false);
                if (statusLabel == null) return;
                StringBuilder sb = new StringBuilder();
                sb.append(parsedCount).append(parsedCount == 1 ? " read" : " reads");
                if (skippedCount > 0) sb.append(", ").append(skippedCount).append(" skipped");
                if (!lastStatus.isEmpty()) sb.append(". ").append(lastStatus);
                statusLabel.setText(sb.toString());
            });
        }
    }

    @Override
    public Boolean chipIsBib() {
        TextLineParser p = parser;
        return p != null && p.idIsBib(preferBib);
    }

    @Override
    public void showControls(Pane p) {
        boolean firstTime = displayVBox == null;
        super.showControls(p);
        if (!firstTime) return;

        formatComboBox = new ComboBox<>(FXCollections.observableArrayList(TextFileFormat.values()));
        formatComboBox.setValue(format);

        customFormatTextField = new TextField(customFormat);
        customFormatTextField.setPromptText("$C,$D,$T");
        Tooltip help = new Tooltip(CUSTOM_FORMAT_HELP);
        help.setShowDuration(Duration.seconds(60));
        help.setStyle("-fx-font-family: monospace;");
        customFormatTextField.setTooltip(help);
        HBox.setHgrow(customFormatTextField, Priority.ALWAYS);

        useAsCustomButton = new Button("Edit as Custom");
        useAsCustomButton.setTooltip(new Tooltip("Copy the detected format into a Custom format so it can be changed"));

        useBibCheckBox = new CheckBox("Use Bib column");
        useBibCheckBox.setSelected(preferBib);
        useBibCheckBox.setTooltip(new Tooltip("The file has both a Chip and a Bib column. When checked the Bib column is used "
                + "as is. Otherwise the Chip column is used and mapped to a bib."));

        formatHBox = new HBox(5, new Label("Format:"), formatComboBox, customFormatTextField, useAsCustomButton, useBibCheckBox);
        formatHBox.setAlignment(Pos.CENTER_LEFT);

        formatLabel = new Label();
        formatLabel.setWrapText(true);
        formatLabel.setMinWidth(0);
        formatLabel.setPrefWidth(1);
        formatLabel.setMaxWidth(Double.MAX_VALUE);

        // Below the file selection row, above the status
        displayVBox.getChildren().add(1, formatHBox);
        displayVBox.getChildren().add(2, formatLabel);

        formatComboBox.valueProperty().addListener((obs, oldValue, newValue) -> {
            if (updatingControls || newValue == null || newValue == format) return;
            if (newValue == TextFileFormat.CUSTOM && !TextLineParser.compileCustom(customFormat).isValid()) {
                // Start the custom format off with what we are using now
                TextFileAutodetect.Result r = detected;
                if (format == TextFileFormat.AUTODETECT && r != null && r.found()) {
                    applySettings(TextFileFormat.CUSTOM, r.customFormat(), preferBib, true);
                    return;
                }
                if (format.isPredefined()) {
                    applySettings(TextFileFormat.CUSTOM, format.customEquivalent(), preferBib, false);
                    return;
                }
            }
            applySettings(newValue, customFormat, preferBib, false);
        });

        customFormatTextField.setOnAction(e -> commitCustomFormat());
        customFormatTextField.focusedProperty().addListener((obs, oldValue, focused) -> {
            if (!focused) commitCustomFormat();
        });

        useBibCheckBox.selectedProperty().addListener((obs, oldValue, newValue) -> {
            if (updatingControls || newValue == preferBib) return;
            applySettings(format, customFormat, newValue, false);
        });

        useAsCustomButton.setOnAction(e -> {
            TextFileAutodetect.Result r = detected;
            if (r == null || !r.found()) return;
            // Same columns as what was detected, so the reads don't change
            applySettings(TextFileFormat.CUSTOM, r.customFormat(), preferBib, true);
        });

        updateFormatControls();
    }

    private void commitCustomFormat() {
        if (updatingControls || format != TextFileFormat.CUSTOM) return;
        String text = customFormatTextField.getText() == null ? "" : customFormatTextField.getText();
        if (text.equals(customFormat)) return;
        applySettings(TextFileFormat.CUSTOM, text, preferBib, false);
    }

    /**
     * Change the format settings. If the input already has reads and the
     * change would read the file differently, ask the user before clearing
     * them and importing the file again. Called on the FX thread.
     *
     * @param newFormat the format
     * @param newCustom the custom format string
     * @param newPreferBib true to use the Bib column when there is also a Chip column
     * @param equivalent true if the change reads the file exactly the same way
     */
    private void applySettings(TextFileFormat newFormat, String newCustom, boolean newPreferBib, boolean equivalent) {
        TextLineParser oldParser = parser;
        TextLineParser newParser = buildParser(newFormat, newCustom, sourceFile);

        // A typo in the custom format: keep using the one we have until it is fixed
        if (newFormat == TextFileFormat.CUSTOM && format == TextFileFormat.CUSTOM
                && !newParser.isValid() && oldParser != null && oldParser.isValid()) {
            formatLabel.setText(newParser.error() + ". Still using " + customFormat);
            formatLabel.setStyle("-fx-text-fill: #c0392b;");
            return;
        }

        boolean changesReads = !equivalent && !sameReads(oldParser, preferBib, newParser, newPreferBib);
        int readCount = timingListener.getReads() == null ? 0 : timingListener.getReads().size();

        boolean clearReads = false;
        if (changesReads && readCount > 0) {
            Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
            alert.setTitle("Change File Format");
            alert.setHeaderText("Clear the existing reads?");
            alert.setContentText("This input already has " + readCount + (readCount == 1 ? " read." : " reads.")
                    + " Changing the format will clear them and import the file again using the new format.");
            ButtonType clear = new ButtonType("Clear and Re-Import", ButtonBar.ButtonData.OK_DONE);
            alert.getButtonTypes().setAll(clear, ButtonType.CANCEL);
            if (formatComboBox != null && formatComboBox.getScene() != null) {
                alert.initOwner(formatComboBox.getScene().getWindow());
            }
            Optional<ButtonType> result = alert.showAndWait();
            if (result.isEmpty() || result.get() != clear) {
                // Put the old settings back
                updateFormatControls();
                return;
            }
            clearReads = true;
        }

        boolean wasTailing = readingStatus.get();

        format = newFormat;
        customFormat = newCustom == null ? "" : newCustom;
        preferBib = newPreferBib;
        parser = newParser;

        timingListener.setAttribute(FORMAT_ATTRIBUTE, format.name());
        timingListener.setAttribute(CUSTOM_FORMAT_ATTRIBUTE, customFormat);
        timingListener.setAttribute(PREFER_BIB_ATTRIBUTE, Boolean.toString(preferBib));
        logger.debug("PikaTextFileReader: now using format {} custom \"{}\" preferBib {}", format, customFormat, preferBib);

        updateFormatControls();

        if (!changesReads) return;
        if (clearReads) timingListener.clearReads();

        // Read the file again from the top with the new format
        if (sourceFile == null || !sourceFile.isFile() || !sourceFile.canRead()) return;
        if (wasTailing) {
            stopReading();
            readingStatus.set(true); // the auto-import switch restarts the Tailer
        } else {
            readOnce();
        }
    }

    // True if both parsers turn the file into the same reads
    private static boolean sameReads(TextLineParser a, boolean aPreferBib, TextLineParser b, boolean bPreferBib) {
        if (a == null || b == null) return a == b;
        if (a.isValid() != b.isValid()) return false;
        return a.format().equals(b.format()) && a.idIsBib(aPreferBib) == b.idIsBib(bPreferBib);
    }

    // Show the controls that apply to the current settings
    private void updateFormatControls() {
        if (formatHBox == null) return;
        updatingControls = true;
        try {
            formatComboBox.setValue(format);
            if (!customFormatTextField.isFocused() || format != TextFileFormat.CUSTOM) {
                customFormatTextField.setText(customFormat);
            }
            useBibCheckBox.setSelected(preferBib);
        } finally {
            updatingControls = false;
        }

        boolean custom = format == TextFileFormat.CUSTOM;
        customFormatTextField.setVisible(custom);
        customFormatTextField.setManaged(custom);

        TextFileAutodetect.Result r = detected;
        boolean canEdit = format == TextFileFormat.AUTODETECT && r != null && r.found();
        useAsCustomButton.setVisible(canEdit);
        useAsCustomButton.setManaged(canEdit);

        TextLineParser p = parser;
        boolean both = p != null && p.isValid() && p.hasBib() && p.hasChip();
        useBibCheckBox.setVisible(both);
        useBibCheckBox.setManaged(both);

        String text;
        boolean error = false;
        if (format == TextFileFormat.AUTODETECT && (sourceFile == null || !sourceFile.isFile())) {
            text = "Select a file to detect its format";
        } else if (format == TextFileFormat.AUTODETECT && (r == null || !r.found())) {
            text = r == null ? "Unable to detect the file format" : r.description();
            error = true;
        } else if (p == null || !p.isValid()) {
            text = p == null ? "Unknown file format" : p.error();
            error = true;
        } else {
            String prefix = format == TextFileFormat.AUTODETECT ? r.description() + ". " : "";
            text = prefix + (p.idIsBib(preferBib)
                    ? "Reading bibs."
                    : "Reading chips (mapped to bibs using the Chip -> Bib map).");
        }
        formatLabel.setText(text);
        formatLabel.setStyle(error ? "-fx-text-fill: #c0392b;" : "");
    }
}
