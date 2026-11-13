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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import javafx.beans.binding.Bindings;
import javafx.beans.binding.BooleanBinding;
import javafx.beans.binding.StringBinding;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.shape.Circle;

/**
 * Controller for TridentDirectReader.fxml, the controls for a Trident
 * TimeMachine timing input.
 *
 * @author John Garner
 */
public class TridentDirectReaderController {

    // Heard from the unit within this many seconds: connected and current
    // (the heartbeat runs every 10 seconds)
    private static final int CURRENT_SECONDS = 12;
    // Battery voltage warnings. The TimeMachine sounds its low battery
    // alarm at about 11.0 V and shuts down at about 10.8 V.
    private static final double BATTERY_WARN = 11.5;
    private static final double BATTERY_LOW = 11.2;
    // Warn if the reader's clock is off by more than this ($GETDATE is in whole seconds)
    private static final double CLOCK_WARN_SECONDS = 1.5;

    private static final Color GREEN = Color.web("#00A37A");   // Vibrant Mint Teal
    private static final Color YELLOW = Color.web("#FFFF33");
    private static final Color RED = Color.web("#FF3300");
    private static final Color IDLE = Color.web("#222222");    // Charcoal Grey

    private TridentDirectReader reader = null;

    @FXML VBox rootVBox;

    @FXML VBox notConfiguredVBox;
    @FXML Button selectReaderButton2;

    @FXML VBox readerVBox;
    @FXML Label unitNameLabel;
    @FXML Label batteryLabel;

    @FXML Label addressLabel;
    @FXML Button selectReaderButton;

    @FXML Label clockLabel;
    @FXML Label onlineStatusLabel;
    @FXML Circle onlineStatusCircle;

    @FXML Label statusLabel;

    @FXML Label replayLabel;
    @FXML Label lastReadLabel;

    @FXML Button rewindButton;
    @FXML Button stopReplayButton;
    @FXML Button syncClockButton;
    @FXML Button disconnectButton;

    public void setReader(TridentDirectReader r) {
        reader = r;

        // Not configured: just show the select button
        notConfiguredVBox.visibleProperty().bind(reader.configuredProperty().not());
        notConfiguredVBox.managedProperty().bind(reader.configuredProperty().not());
        readerVBox.visibleProperty().bind(reader.configuredProperty());
        readerVBox.managedProperty().bind(reader.configuredProperty());

        selectReaderButton.setOnAction(e -> reader.selectReader());
        selectReaderButton2.setOnAction(e -> reader.selectReader());

        // Name and address
        unitNameLabel.textProperty().bind(reader.readerNameProperty());
        addressLabel.textProperty().bind(Bindings.createStringBinding(reader::getAddress,
                reader.readerIPProperty(), reader.dataPortProperty(), reader.commandPortProperty()));
        Tooltip.install(addressLabel, new Tooltip("IP address : data port / command port"));

        // Battery voltage, hidden until the reader sends one
        batteryLabel.textProperty().bind(Bindings.createStringBinding(this::batteryText,
                reader.batteryVoltageProperty(), reader.temperatureProperty()));
        BooleanBinding hasBattery = reader.batteryVoltageProperty().greaterThan(0);
        batteryLabel.visibleProperty().bind(hasBattery);
        reader.batteryVoltageProperty().addListener((ov, o, n) -> updateBattery(n.doubleValue()));
        updateBattery(reader.batteryVoltageProperty().get());
        Tooltip.install(batteryLabel, new Tooltip(
                "Battery voltage from the reader's summary message.\n"
                + "The summary is off by default; enable it in the reader's config.ini.\n"
                + "Low battery alarm at about 11.0 V, shut down at about 10.8 V."));

        // Clock offset
        clockLabel.textProperty().bind(Bindings.createStringBinding(
                () -> "Clock: " + TridentDirectReader.formatOffset(reader.clockOffsetProperty().get()),
                reader.clockOffsetProperty()));
        Tooltip.install(clockLabel, new Tooltip("The reader's clock compared to this computer (+ is ahead)"));

        // Online status
        reader.lastUpdatedProperty().addListener((ov, o, n) -> updateOnline());
        reader.enabledProperty().addListener((ov, o, n) -> updateOnline());
        reader.connectedProperty().addListener((ov, o, n) -> updateOnline());
        updateOnline();

        // Status line: why we are in an error state
        StringBinding status = Bindings.createStringBinding(this::statusText,
                reader.enabledProperty(),
                reader.connectedProperty(),
                reader.batteryVoltageProperty(),
                reader.clockOffsetProperty(),
                reader.commandProblemProperty());
        statusLabel.textProperty().bind(status);
        statusLabel.visibleProperty().bind(status.isNotEmpty());
        statusLabel.managedProperty().bind(status.isNotEmpty());
        status.addListener((ov, o, n) -> updateErrorState(n));
        updateErrorState(status.get());

        // Replay progress and the last read
        replayLabel.textProperty().bind(Bindings.createStringBinding(this::replayText,
                reader.replayActiveProperty(), reader.replayCountProperty()));
        lastReadLabel.textProperty().bind(reader.lastReadProperty());

        // Buttons
        rewindButton.setOnAction(event -> reader.rewind());
        rewindButton.disableProperty().bind(reader.connectedProperty().not());

        stopReplayButton.setOnAction(event -> reader.stopReplay());
        stopReplayButton.visibleProperty().bind(reader.replayActiveProperty());
        stopReplayButton.managedProperty().bind(reader.replayActiveProperty());

        syncClockButton.setOnAction(event -> reader.syncClock());
        syncClockButton.disableProperty().bind(reader.connectedProperty().not());

        disconnectButton.textProperty().bind(Bindings.when(reader.enabledProperty()).then("Disconnect").otherwise("Reconnect"));
        disconnectButton.setOnAction(event -> reader.toggleConnection());
    }

    private String batteryText() {
        double volts = reader.batteryVoltageProperty().get();
        if (volts <= 0) return "";
        String text = String.format("Batt: %.1f V", volts);
        double temp = reader.temperatureProperty().get();
        if (!Double.isNaN(temp)) text += String.format("  %.0f°", temp);
        return text;
    }

    private void updateBattery(double volts) {
        batteryLabel.getStyleClass().remove("warning-label");
        if (volts > 0 && volts <= BATTERY_LOW) {
            batteryLabel.setStyle("");
            batteryLabel.getStyleClass().add("warning-label");
        } else if (volts > 0 && volts <= BATTERY_WARN) {
            batteryLabel.setStyle("-fx-text-fill: #b8a000;"); // dark yellow, readable on light backgrounds
        } else {
            batteryLabel.setStyle("");
        }
    }

    private boolean clockOff() {
        Duration offset = reader.clockOffsetProperty().get();
        return offset != null && Math.abs(offset.toMillis()) > CLOCK_WARN_SECONDS * 1000;
    }

    private void updateOnline() {
        int lastUpdated = reader.lastUpdatedProperty().get();
        if (!reader.enabledProperty().get()) {
            onlineStatusCircle.setFill(IDLE);
            onlineStatusLabel.setText("Disconnected:");
        } else if (!reader.connectedProperty().get()) {
            onlineStatusCircle.setFill(RED);
            onlineStatusLabel.setText("Not Connected:");
        } else if (lastUpdated <= CURRENT_SECONDS) {
            onlineStatusCircle.setFill(GREEN);
            onlineStatusLabel.setText("Connected:");
        } else {
            // The data connection is up, but the unit has been quiet
            // (no reads and no heartbeat replies)
            onlineStatusCircle.setFill(YELLOW);
            String seen = lastUpdated >= TridentDirectReader.LAST_UPDATED_MAX ? "never" : lastUpdated + "s";
            onlineStatusLabel.setText("Last Seen: " + seen);
        }
    }

    // The status line: the reasons for an error state,
    // e.g. "Clock off by +3.0s · Low Battery: 11.1 V". "" hides the line.
    private String statusText() {
        // The user disconnected it, not an error
        if (!reader.enabledProperty().get()) return "Disconnected";

        List<String> parts = new ArrayList<>();
        if (!reader.connectedProperty().get()) {
            parts.add("Not connected");
        } else {
            String commandProblem = reader.commandProblemProperty().getValueSafe();
            if (!commandProblem.isBlank()) parts.add(commandProblem);
            if (clockOff()) parts.add("Clock off by " + TridentDirectReader.formatOffset(reader.clockOffsetProperty().get()));
        }

        // The last known level is kept while not connected, as a dead
        // battery is the usual reason
        double volts = reader.batteryVoltageProperty().get();
        if (volts > 0 && volts <= BATTERY_LOW) parts.add(String.format("Low Battery: %.1f V", volts));
        return String.join(" · ", parts);
    }

    // Every status line other than a deliberate disconnect is a problem
    private void updateErrorState(String status) {
        boolean error = reader.enabledProperty().get() && status != null && !status.isEmpty();
        if (error) {
            if (!statusLabel.getStyleClass().contains("warning-label")) statusLabel.getStyleClass().add("warning-label");
        } else {
            statusLabel.getStyleClass().remove("warning-label");
        }
    }

    private String replayText() {
        int count = reader.replayCountProperty().get();
        if (count < 0) return "";
        if (reader.replayActiveProperty().get()) return "Rewinding: " + count + " reads";
        return "Rewind: " + count + " reads";
    }
}
