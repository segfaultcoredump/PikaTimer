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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.beans.binding.BooleanBinding;
import javafx.beans.binding.StringBinding;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.collections.FXCollections;
import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressBar;
import javafx.scene.control.Tooltip;
import javafx.scene.input.MouseButton;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.shape.Circle;
import org.json.JSONArray;
import org.json.JSONObject;
import org.pikatimer.Pikatimer;
import org.pikatimer.timing.reader.PikaReaderDirectReader.Profile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Controller for PikaReaderDirectReader.fxml, the controls for a PikaReader
 * timing input.
 *
 * @author John Garner
 */
public class PikaReaderDirectReaderController {
    private static final Logger logger = LoggerFactory.getLogger(PikaReaderDirectReaderController.class);

    // Error state thresholds: no STATUS for this many seconds, or a battery
    // level (0-1) at or below this
    private static final int OFFLINE_SECONDS = 10;
    private static final double LOW_BATTERY = 0.20;

    private static final Color GREEN = Color.web("#00A37A");   // Vibrant Mint Teal
    private static final Color YELLOW = Color.web("#FFFF33");
    private static final Color RED = Color.web("#FF3300");
    private static final Color IDLE = Color.web("#222222");    // Charcoal Grey

    private PikaReaderDirectReader reader = null;

    @FXML VBox rootVBox;

    @FXML VBox notConfiguredVBox;
    @FXML Button selectReaderButton2;

    @FXML VBox readerVBox;
    @FXML Label unitNameLabel;
    @FXML HBox battHBox;
    @FXML ProgressBar batteryLevelProgressBar;

    @FXML Label addressLabel;
    @FXML Button selectReaderButton;

    @FXML Label readStatusLabel;
    @FXML Circle readStatusCircle;
    @FXML Label onlineStatusLabel;
    @FXML Circle onlineStatusCircle;

    @FXML Label statusLabel;

    @FXML HBox profileHBox;
    @FXML ChoiceBox<Profile> profileChoiceBox;

    @FXML Button startReaderButton;
    @FXML Label lastReadLabel;

    @FXML Button rewindButton;
    @FXML Button disconnectButton;

    // The ChoiceBox gets its own copy of the reader's profile list so that
    // every programmatic update happens under the syncingProfile guard and
    // never looks like the user picking a new profile
    private final ObservableList<Profile> profileChoices = FXCollections.observableArrayList();
    private final BooleanProperty profileChangeInProgress = new SimpleBooleanProperty(false);
    private boolean syncingProfile = false;

    private final BooleanProperty errorState = new SimpleBooleanProperty(false);

    @FXML
    public void initialize() {
        // Flag the status line if we have an issue
        errorState.addListener((observable, oldValue, newValue) -> {
            if (newValue) {
                if (!statusLabel.getStyleClass().contains("warning-label")) statusLabel.getStyleClass().add("warning-label");
            } else {
                statusLabel.getStyleClass().remove("warning-label");
            }
        });
    }

    public void setReader(PikaReaderDirectReader r) {
        reader = r;

        // Not configured: just show the select button
        notConfiguredVBox.visibleProperty().bind(reader.configuredProperty().not());
        notConfiguredVBox.managedProperty().bind(reader.configuredProperty().not());
        readerVBox.visibleProperty().bind(reader.configuredProperty());
        readerVBox.managedProperty().bind(reader.configuredProperty());

        selectReaderButton.setOnAction(e -> reader.discover());
        selectReaderButton2.setOnAction(e -> reader.discover());

        // Name and address. Double click to open the unit's web page
        unitNameLabel.textProperty().bind(reader.readerNameProperty());
        addressLabel.textProperty().bind(Bindings.createStringBinding(reader::getBaseURL,
                reader.secureProperty(), reader.readerIPProperty(), reader.readerPortProperty()));
        addressLabel.setOnMouseClicked(event -> {
            if (event.getButton() == MouseButton.PRIMARY && event.getClickCount() == 2) {
                String url = reader.getBaseURL();
                logger.debug("Opening {}", url);
                Pikatimer.getInstance().getHostServices().showDocument(url);
            }
        });
        Tooltip.install(addressLabel, new Tooltip("Double click to open the reader's web page"));

        // Battery Level
        batteryLevelProgressBar.progressProperty().addListener((observable, oldValue, newValue) -> updateBattery(newValue.doubleValue()));
        batteryLevelProgressBar.progressProperty().bind(reader.batteryProperty());
        updateBattery(reader.batteryProperty().get());

        // Reading status
        updateReading(reader.readingProperty().get());
        reader.readingProperty().addListener((ov, o, n) -> updateReading(n));

        // Online status
        updateOnline(reader.lastUpdatedProperty().get());
        reader.lastUpdatedProperty().addListener((ov, o, n) -> updateOnline(n.intValue()));
        reader.enabledProperty().addListener((ov, o, n) -> updateOnline(reader.lastUpdatedProperty().get()));

        // Status line: why we are in an error state, and the battery level
        StringBinding status = Bindings.createStringBinding(this::statusText,
                reader.enabledProperty(),
                reader.lastUpdatedProperty(),
                reader.batteryProperty(),
                reader.rfidReaderProblemProperty());
        statusLabel.textProperty().bind(status);
        statusLabel.visibleProperty().bind(status.isNotEmpty());
        statusLabel.managedProperty().bind(status.isNotEmpty());

        // Last read
        lastReadLabel.textProperty().bind(reader.lastReadProperty());

        // Reader Profile
        // Hidden if the reader is running an older PikaReader without profiles
        profileHBox.visibleProperty().bind(reader.profileSupportedProperty());
        profileHBox.managedProperty().bind(reader.profileSupportedProperty());
        profileChoiceBox.setItems(profileChoices);

        // Visible but not changeable while reading
        profileChoiceBox.disableProperty().bind(
                reader.readingProperty()
                        .or(profileChangeInProgress)
                        .or(reader.connectedProperty().not())
                        .or(Bindings.isEmpty(profileChoices)));

        // Tooltips are not shown on disabled controls, so put it on the HBox
        Tooltip profileTooltip = new Tooltip();
        profileTooltip.textProperty().bind(
                Bindings.when(reader.readingProperty())
                        .then("Stop the reader to change the profile")
                        .otherwise("Select a reader profile"));
        Tooltip.install(profileHBox, profileTooltip);

        // Keep the display in sync with the reader, including changes made
        // directly on the unit (via the STATUS messages)
        syncProfileList();
        reader.getProfiles().addListener((ListChangeListener.Change<? extends Profile> c) -> syncProfileList());
        reader.profileIDProperty().addListener((ov, o, n) -> syncProfileSelection());

        // The user picked a profile
        profileChoiceBox.valueProperty().addListener((ov, o, n) -> {
            if (syncingProfile || n == null) return;
            if (n.id().equals(reader.profileIDProperty().getValueSafe())) return;
            // Let the ChoiceBox popup close before we prompt
            Platform.runLater(() -> promptProfileChange(n));
        });

        // Buttons
        startReaderButton.setOnAction(event -> toggleReading());
        rewindButton.setOnAction(event -> reader.rewind());
        rewindButton.disableProperty().bind(reader.connectedProperty().not());

        disconnectButton.textProperty().bind(Bindings.when(reader.enabledProperty()).then("Disconnect").otherwise("Reconnect"));
        disconnectButton.setOnAction(event -> reader.toggleConnection());

        // Error state if the battery is at or below 20%, we have not heard
        // from the unit in 10 seconds, or one of its RFID readers is not
        // connected, unless the unit is disconnected on purpose
        // (keep in sync with statusText())
        errorState.bind(
                reader.enabledProperty().and(
                        reader.lastUpdatedProperty().greaterThan(OFFLINE_SECONDS)
                                .or(reader.batteryProperty().greaterThanOrEqualTo(0.0)
                                        .and(reader.batteryProperty().lessThanOrEqualTo(LOW_BATTERY)))
                                .or(reader.rfidReaderProblemProperty().isNotEmpty())));
    }

    private void updateBattery(double progress) {
        battHBox.setVisible(progress >= 0);
        if (progress < LOW_BATTERY) {
            batteryLevelProgressBar.setStyle("-fx-accent: #FF3300;"); // Red
        } else if (progress > 0.50) {
            batteryLevelProgressBar.setStyle("-fx-accent: #00A37A;"); // Vibrant Mint Teal
        } else {
            batteryLevelProgressBar.setStyle("-fx-accent: #dede17;"); // Yellow
        }
    }

    private void updateReading(boolean reading) {
        if (reading) {
            readStatusCircle.setFill(GREEN);
            startReaderButton.setText("Stop Reader");
            readStatusLabel.setText("Reader: Reading");
        } else {
            readStatusCircle.setFill(IDLE);
            startReaderButton.setText("Start Reader");
            readStatusLabel.setText("Reader: Idle");
        }
    }

    private void updateOnline(int lastUpdated) {
        if (!reader.enabledProperty().get()) {
            onlineStatusCircle.setFill(IDLE);
            onlineStatusLabel.setText("Disconnected:");
        } else if (lastUpdated < 2) {
            onlineStatusCircle.setFill(GREEN);
            onlineStatusLabel.setText("Connected:");
        } else if (lastUpdated < OFFLINE_SECONDS) {
            onlineStatusCircle.setFill(YELLOW);
            onlineStatusLabel.setText("Last Seen: " + lastUpdated + "s");
        } else {
            onlineStatusCircle.setFill(RED);
            onlineStatusLabel.setText("Not Responding:");
        }
    }

    // The status line: the reasons for an error state,
    // e.g. "Not responding · Low Battery: 15%". "" hides the line.
    private String statusText() {
        // The user disconnected it, not an error
        if (!reader.enabledProperty().get()) return "Disconnected";

        List<String> parts = new ArrayList<>();
        if (reader.lastUpdatedProperty().get() > OFFLINE_SECONDS) {
            parts.add("Not responding");
        } else {
            // Only current while we are hearing from the unit
            String rfidProblem = reader.rfidReaderProblemProperty().getValueSafe();
            if (!rfidProblem.isBlank()) parts.add(rfidProblem);
        }

        // The battery bar shows the level, so only call out a low battery
        // (an error state). Below 0: the unit does not report a battery level
        // (or no STATUS yet). The last known level is kept while not
        // responding, as a dead battery is the usual reason.
        double battery = reader.batteryProperty().get();
        if (battery >= 0 && battery <= LOW_BATTERY) {
            parts.add("Low Battery: " + Math.round(battery * 100) + "%");
        }
        return String.join(" · ", parts);
    }

    private void toggleReading() {
        boolean reading = reader.readingProperty().get();
        String title = reading ? "Stop Reader" : "Start Reader";
        String action = reading ? "stop" : "start";
        String name = reader.readerNameProperty().getValueSafe();

        Alert alert = new Alert(AlertType.CONFIRMATION);
        alert.setTitle(title);
        alert.setHeaderText(title);
        alert.setContentText("This will " + action + " the reader " + name + ".\nAre you ok with this?");
        Optional<ButtonType> result = alert.showAndWait();
        if (result.isEmpty() || result.get() != ButtonType.OK) return;

        startReaderButton.setDisable(true);
        CompletableFuture<?> request = reading ? reader.stopReader() : reader.startReader();
        request.whenComplete((response, ex) -> Platform.runLater(() -> {
            startReaderButton.setDisable(false);
            if (ex != null) {
                String msg = ReaderUtil.errorMessage(ex);
                logger.warn("Unable to {} {}: {}", action, name, msg);
                Alert error = new Alert(AlertType.ERROR);
                error.setTitle(title);
                error.setHeaderText("Unable to " + action + " " + name);
                error.setContentText(msg);
                error.showAndWait();
            }
        }));
    }

    // Copy the reader's profile list into the ChoiceBox and select the current one
    private void syncProfileList() {
        syncingProfile = true;
        try {
            profileChoices.setAll(reader.getProfiles());
        } finally {
            syncingProfile = false;
        }
        syncProfileSelection();
    }

    // Select the profile the reader is currently using
    private void syncProfileSelection() {
        syncingProfile = true;
        try {
            String currentID = reader.profileIDProperty().getValueSafe();
            Profile current = profileChoices.stream()
                    .filter(p -> p.id().equals(currentID))
                    .findFirst()
                    .orElse(null);
            profileChoiceBox.setValue(current);
        } finally {
            syncingProfile = false;
        }
    }

    private void promptProfileChange(Profile target) {
        String name = reader.readerNameProperty().getValueSafe();
        String currentName = reader.profileNameProperty().getValueSafe();

        Alert alert = new Alert(AlertType.CONFIRMATION);
        alert.setTitle("Change Reader Profile");
        alert.setHeaderText("Change Reader Profile");
        String content = "Change the reader profile for " + name
                + "\nfrom " + currentName + " to " + target.name() + "?";
        if (target.description() != null && !target.description().isBlank()) {
            content += "\n\n" + target.name() + ": " + target.description();
        }
        alert.setContentText(content);

        Optional<ButtonType> result = alert.showAndWait();
        if (result.isEmpty() || result.get() != ButtonType.OK) {
            syncProfileSelection(); // revert the ChoiceBox
            return;
        }

        profileChangeInProgress.set(true);
        reader.changeProfile(target.id()).whenComplete((response, ex) -> Platform.runLater(() -> {
            profileChangeInProgress.set(false);
            syncProfileSelection();

            if (ex != null) {
                String msg = ReaderUtil.errorMessage(ex);
                logger.warn("Unable to change the profile for {} to {}: {}", name, target.id(), msg);
                Alert error = new Alert(AlertType.ERROR);
                error.setTitle("Change Reader Profile");
                error.setHeaderText("Unable to change the reader profile");
                error.setContentText(msg);
                error.showAndWait();
                return;
            }

            // Show any warnings (e.g. a reader model not suited to this profile)
            List<String> warnings = profileWarnings(response);
            if (!warnings.isEmpty()) {
                Alert warning = new Alert(AlertType.WARNING);
                warning.setTitle("Change Reader Profile");
                warning.setHeaderText("Profile changed to " + reader.profileNameProperty().getValueSafe());
                warning.setContentText(String.join("\n", warnings));
                warning.showAndWait();
            }
        }));
    }

    // {"readers":[{"id":1,"settings":{"warnings":["..."]}},...]}
    private static List<String> profileWarnings(JSONObject response) {
        List<String> warnings = new ArrayList<>();
        if (response == null) return warnings;
        JSONArray readers = response.optJSONArray("readers");
        if (readers == null) return warnings;
        for (int i = 0; i < readers.length(); i++) {
            JSONObject r = readers.optJSONObject(i);
            if (r == null) continue;
            JSONObject settings = r.optJSONObject("settings");
            if (settings == null) continue;
            JSONArray w = settings.optJSONArray("warnings");
            if (w == null) continue;
            for (int j = 0; j < w.length(); j++) {
                String s = w.optString(j, "");
                if (!s.isBlank()) warnings.add(s);
            }
        }
        return warnings;
    }
}
