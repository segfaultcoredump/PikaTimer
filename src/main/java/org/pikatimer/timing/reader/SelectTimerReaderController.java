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

import com.google.zxing.BarcodeFormat;
import com.google.zxing.EncodeHintType;
import com.google.zxing.common.BitMatrix;
import com.google.zxing.qrcode.QRCodeWriter;
import com.google.zxing.qrcode.decoder.ErrorCorrectionLevel;
import java.util.Map;
import java.util.Optional;
import javafx.beans.binding.Bindings;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.beans.property.ReadOnlyStringWrapper;
import javafx.collections.ListChangeListener;
import javafx.fxml.FXML;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.Spinner;
import javafx.scene.control.SpinnerValueFactory;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.Tooltip;
import javafx.scene.image.ImageView;
import javafx.scene.image.PixelWriter;
import javafx.scene.image.WritableImage;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.shape.Circle;
import org.pikatimer.timing.reader.SelectTimerReader.DeviceRow;
import org.pikatimer.util.HTTPServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Controller for SelectTimerReader.fxml, the controls for an automatic
 * select timer input.
 *
 * @author John Garner
 */
public class SelectTimerReaderController {
    private static final Logger logger = LoggerFactory.getLogger(SelectTimerReaderController.class);

    private static final Color GREEN = Color.web("#00A37A");
    private static final Color IDLE = Color.web("#222222");

    private SelectTimerReader reader = null;

    @FXML VBox rootVBox;
    @FXML Label statusLabel;
    @FXML Circle statusCircle;
    @FXML ComboBox<String> urlComboBox;
    @FXML Button copyButton;
    @FXML ImageView qrImageView;
    @FXML Label helpLabel;
    @FXML Label warningLabel;
    @FXML CheckBox knownBibsCheckBox;
    @FXML Spinner<Integer> repeatSpinner;
    @FXML Button newLinkButton;
    @FXML Button enableButton;
    @FXML Label readCountLabel;
    @FXML Label lastReadLabel;
    @FXML TableView<DeviceRow> deviceTableView;
    @FXML TableColumn<DeviceRow, String> deviceNameColumn;
    @FXML TableColumn<DeviceRow, String> deviceAddressColumn;
    @FXML TableColumn<DeviceRow, String> deviceContactColumn;
    @FXML TableColumn<DeviceRow, Integer> deviceReadsColumn;
    @FXML TableColumn<DeviceRow, Integer> devicePendingColumn;
    @FXML TableColumn<DeviceRow, String> deviceClockColumn;

    public void setReader(SelectTimerReader r) {
        reader = r;

        // Link: one per network address, the selected one is shown as a QR code
        urlComboBox.setItems(reader.getUrls());
        reader.getUrls().addListener((ListChangeListener<String>) c -> selectUrl());
        urlComboBox.valueProperty().addListener((ov, o, n) -> showQRCode(n));
        selectUrl();
        Tooltip urlTip = new Tooltip();
        urlTip.textProperty().bind(Bindings.createStringBinding(
                () -> "Open this link on each phone or tablet. If this computer has more than one\n"
                + "network address, pick the one on the same network as the devices.\n\n"
                + "Certificate fingerprint (SHA-256):\n" + reader.fingerprintProperty().get(),
                reader.fingerprintProperty()));
        urlComboBox.setTooltip(urlTip);

        copyButton.setOnAction(e -> {
            String url = urlComboBox.getValue();
            if (url == null) return;
            ClipboardContent content = new ClipboardContent();
            content.putString(url);
            Clipboard.getSystemClipboard().setContent(content);
        });
        copyButton.disableProperty().bind(urlComboBox.valueProperty().isNull());

        // HTTPS is needed for the camera
        if (HTTPServices.getInstance().httpsPort() <= 0) {
            ReaderUtil.setStatus(warningLabel, "HTTPS is not available (see the log), so phones and tablets "
                    + "will not be able to use their camera.", "warning-label");
            warningLabel.setVisible(true);
            warningLabel.setManaged(true);
        }

        // Accepting reads
        reader.getReadingStatus().addListener((ov, o, n) -> updateStatus());
        updateStatus();
        enableButton.textProperty().bind(Bindings.when(reader.enabledProperty())
                .then("Stop Accepting Reads").otherwise("Accept Reads"));
        enableButton.setOnAction(e -> reader.toggleEnabled());
        Tooltip.install(enableButton, new Tooltip(
                "While stopped, the devices keep their reads queued and send them once reads are accepted again."));

        // Settings
        knownBibsCheckBox.setSelected(reader.knownBibsOnlyProperty().get());
        reader.knownBibsOnlyProperty().addListener((ov, o, n) -> knownBibsCheckBox.setSelected(n));
        knownBibsCheckBox.selectedProperty().addListener((ov, o, n) -> reader.setKnownBibsOnly(n));
        knownBibsCheckBox.setTooltip(new Tooltip(
                "Ignore numbers that are not a registered bib. This filters out most OCR mistakes.\n"
                + "The devices refresh the list of bibs every minute (or with their Refresh Bibs button).\n"
                + "Turn this off for day of registrations that are not in PikaTimer yet."));

        SpinnerValueFactory.IntegerSpinnerValueFactory repeatFactory = new SpinnerValueFactory.IntegerSpinnerValueFactory(
                0, SelectTimerReader.MAX_REPEAT_SECONDS, reader.repeatSecondsProperty().get(), 5);
        repeatSpinner.setValueFactory(repeatFactory);
        reader.repeatSecondsProperty().addListener((ov, o, n) -> repeatFactory.setValue(n.intValue()));
        repeatSpinner.valueProperty().addListener((ov, o, n) -> {
            if (n != null) reader.setRepeatSeconds(n);
        });
        // Commit typed values when focus leaves the field
        repeatSpinner.focusedProperty().addListener((ov, o, n) -> {
            if (!n) commitSpinner();
        });
        repeatSpinner.setTooltip(new Tooltip(
                "Once a device reports a bib it ignores that bib for this long, so a runner standing\n"
                + "in the camera's view is only reported once."));

        newLinkButton.setOnAction(e -> confirmNewLink());

        // Reads
        readCountLabel.textProperty().bind(Bindings.createStringBinding(
                () -> "Reads: " + reader.readCountProperty().get(), reader.readCountProperty()));
        lastReadLabel.textProperty().bind(reader.lastReadProperty());

        // Devices
        deviceTableView.setItems(reader.getDevices());
        deviceTableView.setPlaceholder(new Label("No devices have connected yet"));
        deviceNameColumn.setCellValueFactory(c -> new ReadOnlyStringWrapper(c.getValue().name()));
        deviceAddressColumn.setCellValueFactory(c -> new ReadOnlyStringWrapper(c.getValue().address()));
        deviceContactColumn.setCellValueFactory(c -> new ReadOnlyStringWrapper(c.getValue().lastContact()));
        deviceReadsColumn.setCellValueFactory(c -> new ReadOnlyObjectWrapper<>(c.getValue().reads()));
        devicePendingColumn.setCellValueFactory(c -> new ReadOnlyObjectWrapper<>(c.getValue().pending()));
        deviceClockColumn.setCellValueFactory(c -> new ReadOnlyStringWrapper(c.getValue().clock()));
        Tooltip.install(deviceTableView, new Tooltip(
                "Queued: reads the device has not been able to send yet.\n"
                + "Clock: the device's clock compared to this computer (+ is ahead). The device\n"
                + "corrects its reads to this computer's clock, so this is for information only."));
    }

    private void commitSpinner() {
        try {
            String text = repeatSpinner.getEditor().getText();
            int v = Integer.parseInt(text.trim());
            repeatSpinner.getValueFactory().setValue(Math.max(0, Math.min(v, SelectTimerReader.MAX_REPEAT_SECONDS)));
        } catch (NumberFormatException ex) {
            repeatSpinner.getEditor().setText(String.valueOf(repeatSpinner.getValue()));
        }
    }

    private void selectUrl() {
        String current = urlComboBox.getValue();
        if (current != null && reader.getUrls().contains(current)) return;
        if (reader.getUrls().isEmpty()) {
            urlComboBox.setValue(null);
            return;
        }
        // Keep the same address if only the token changed
        if (current != null) {
            String host = current.replaceAll("^https?://([^/]+)/.*$", "$1");
            for (String u : reader.getUrls()) {
                if (u.contains("://" + host + "/")) {
                    urlComboBox.setValue(u);
                    return;
                }
            }
        }
        urlComboBox.setValue(reader.getUrls().get(0));
    }

    private void updateStatus() {
        boolean reading = reader.getReadingStatus().get();
        statusCircle.setFill(reading ? GREEN : IDLE);
    }

    private void confirmNewLink() {
        Alert alert = new Alert(AlertType.CONFIRMATION);
        alert.setTitle("New Link");
        alert.setHeaderText("Make a new link for this input?");
        alert.setContentText("The current link will stop working. Every device using it will need to open the new link.\n\n"
                + "Reads a device has not sent yet stay queued on that device until it opens the old link again, "
                + "so make sure the devices have sent everything first.");
        Optional<ButtonType> result = alert.showAndWait();
        if (result.isPresent() && result.get() == ButtonType.OK) reader.regenerateToken();
    }

    private void showQRCode(String text) {
        if (text == null || text.isEmpty()) {
            qrImageView.setImage(null);
            return;
        }
        try {
            BitMatrix matrix = new QRCodeWriter().encode(text, BarcodeFormat.QR_CODE, 0, 0,
                    Map.of(EncodeHintType.MARGIN, 2, EncodeHintType.ERROR_CORRECTION, ErrorCorrectionLevel.M));
            int scale = Math.max(1, (int) (qrImageView.getFitWidth() / matrix.getWidth()));
            int size = matrix.getWidth() * scale;
            WritableImage image = new WritableImage(size, size);
            PixelWriter pw = image.getPixelWriter();
            for (int y = 0; y < size; y++) {
                for (int x = 0; x < size; x++) {
                    pw.setColor(x, y, matrix.get(x / scale, y / scale) ? Color.BLACK : Color.WHITE);
                }
            }
            qrImageView.setImage(image);
        } catch (Exception ex) {
            logger.warn("Unable to make the QR code", ex);
            qrImageView.setImage(null);
        }
    }
}
