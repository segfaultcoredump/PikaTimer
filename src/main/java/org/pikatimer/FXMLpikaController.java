/* 
 * Copyright (C) 2024 John Garner
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

import org.pikatimer.event.Event;
import org.pikatimer.results.ResultsDAO;
import javafx.animation.PauseTransition;
import javafx.beans.binding.Bindings;
import javafx.fxml.FXML;
import javafx.scene.control.Label;
import javafx.scene.control.TabPane;
import javafx.scene.layout.HBox;
import javafx.util.Duration;


/**
 * FXML Controller class
 *
 * @author jcgarner
 */
public class FXMLpikaController   {
    
    private final Event event = Event.getInstance();
    @FXML private Label eventName;
    @FXML private Label eventDate;
    @FXML private TabPane mainTabPane; 
    @FXML private HBox busyIndicator;
    /**
     * Initializes the controller class.
     */
    @FXML
    protected void initialize() {
        // TODO
        eventName.textProperty().bind(Bindings.concat("PikaTimer: ").concat(event.eventNameProperty()));
        eventDate.textProperty().bind(event.eventDateStringProperty());
        event.setMainTabPane(mainTabPane);
        
        // Let the user know when a large recalculation is in progress. 
        // Hiding is delayed a bit so that it does not flicker between the 
        // stages of a single operation (reprocess the times, then the results). 
        PauseTransition hideBusy = new PauseTransition(Duration.millis(500));
        hideBusy.setOnFinished(e -> busyIndicator.setVisible(false));
        ResultsDAO.getInstance().busyProperty().addListener((obs, wasBusy, isBusy) -> {
            if (isBusy) {
                hideBusy.stop();
                busyIndicator.setVisible(true);
            } else {
                hideBusy.playFromStart();
            }
        });
    }
    
    
       
}
