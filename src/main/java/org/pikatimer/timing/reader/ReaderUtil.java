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

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.http.HttpTimeoutException;
import java.nio.channels.UnresolvedAddressException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import javafx.application.Platform;
import javafx.beans.property.BooleanProperty;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import org.pikatimer.util.DurationParser;

/**
 * Helpers shared by the direct (network) timing readers.
 *
 * @author John Garner
 */
final class ReaderUtil {

    private ReaderUtil() {
    }

    // Run now if we are on the FX thread, otherwise queue it
    static void runOnFX(Runnable r) {
        if (Platform.isFxApplicationThread()) r.run();
        else Platform.runLater(r);
    }

    // Date + time of day (a duration from midnight, so 24:00:00 rolls over to the next day)
    static LocalDateTime toDateTime(LocalDate date, String time) {
        return date.atStartOfDay().plus(DurationParser.parse(time));
    }

    // Only allow time-like input (HH:MM:SS.sss) in the field, and track if it is a valid time
    static void installTimeFilter(TextField timeField, BooleanProperty timeOK) {
        timeField.textProperty().addListener((observable, oldValue, newValue) -> {
            timeOK.set(DurationParser.parsable(newValue));
            if (!(newValue.isEmpty() || newValue.matches("^[0-9]*(:?([0-5]?([0-9]?(:([0-5]?([0-9]?)?)?)?)?)?)?"))) {
                Platform.runLater(() -> {
                    int c = timeField.getCaretPosition();
                    if (oldValue.length() > newValue.length()) c++;
                    else c--;
                    timeField.setText(oldValue);
                    timeField.positionCaret(c);
                });
            }
        });
    }

    // Sets a status label's text, colored with a style class from
    // PikaReaderDirect.css (ok-label, warning-label) or plain if null
    static void setStatus(Label label, String text, String styleClass) {
        label.getStyleClass().removeAll("ok-label", "warning-label");
        if (styleClass != null) label.getStyleClass().add(styleClass);
        label.setText(text);
    }

    /**
     * @param ex an exception, possibly wrapped by a CompletableFuture
     * @return a message suitable for showing to the user
     */
    static String errorMessage(Throwable ex) {
        Throwable cause = ex;
        while ((cause instanceof CompletionException || cause instanceof ExecutionException) && cause.getCause() != null) {
            cause = cause.getCause();
        }
        if (cause instanceof HttpTimeoutException) return "The reader did not respond in time";
        if (cause instanceof SocketTimeoutException) return "The reader did not respond in time";
        if (cause instanceof TimeoutException) return "The reader did not respond in time";
        if (cause instanceof UnknownHostException) return "Unknown host";
        if (cause instanceof ConnectException) {
            if (cause.getCause() instanceof UnresolvedAddressException) return "Unknown host";
            return "Unable to connect to the reader";
        }
        if (cause instanceof javax.net.ssl.SSLException) return "Secure connection failed: " + cause.getMessage();
        return cause.getMessage() != null ? cause.getMessage() : cause.toString();
    }
}
