/*
 * Copyright (C) 2024 John Garner <segfaultcoredump@gmail.com>
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
package org.pikatimer;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.FormatStyle;
import java.util.Properties;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.Hyperlink;
import javafx.scene.control.Label;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FXML Controller class
 *
 * @author John Garner <segfaultcoredump@gmail.com>
 */
public class FXMLAboutController{
    private static final Logger logger = LoggerFactory.getLogger(FXMLAboutController.class);


    @FXML Label pikaVersionLabel;
    @FXML Label pikaBuildLabel;

    /**
     * Initializes the controller class.
     */
    public void initialize() {
        pikaVersionLabel.setText("Version " + Pikatimer.VERSION);

        String buildTime = getBuildTime();
        if (buildTime != null) {
            pikaBuildLabel.setText("Built " + buildTime);
        } else {
            pikaBuildLabel.setVisible(false);
            pikaBuildLabel.setManaged(false);
        }
    }

    /**
     * The build time recorded by Maven in build.properties, formatted in the
     * local time zone, or null if it isn't available (e.g. a build that skipped
     * Maven's resource filtering).
     */
    private String getBuildTime() {
        try (InputStream in = getClass().getResourceAsStream("/org/pikatimer/build.properties")) {
            if (in == null) {
                logger.debug("build.properties not found");
                return null;
            }
            Properties props = new Properties();
            props.load(in);
            Instant built = Instant.parse(props.getProperty("build.timestamp", "").trim());
            return DateTimeFormatter.ofLocalizedDateTime(FormatStyle.MEDIUM)
                    .withZone(ZoneId.systemDefault())
                    .format(built);
        } catch (IOException | DateTimeParseException e) {
            logger.debug("Unable to read the build timestamp", e);
            return null;
        }
    }
    
    @FXML
    protected void openLink(ActionEvent fxevent) {
        Hyperlink hyperlink = (Hyperlink)fxevent.getSource();
        String link = hyperlink.getText();
        if (link.contains("(")) {
            link = link.replaceFirst("^.+\\(", "").replaceFirst("\\).*$", "");
        }
        logger.trace("Hyperlink pressed: " + link);
        Pikatimer.getInstance().getHostServices().showDocument(link);
    }
    
}
