/* 
 * Copyright (C) 2017 John Garner
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
package org.pikatimer.timing;

import org.pikatimer.participant.Participant;
import org.pikatimer.participant.ParticipantDAO;
import org.pikatimer.race.RaceDAO;
import org.pikatimer.race.Race;
import org.pikatimer.race.Wave;
import org.pikatimer.results.ResultsDAO;
import org.pikatimer.util.AlphanumericComparator;
import org.pikatimer.util.BackgroundSortedFilteredList;
import org.pikatimer.util.DurationFormatter;
import org.pikatimer.util.DurationStringConverter;
import org.pikatimer.util.TemporalField;
import org.pikatimer.util.TextFieldFormatters;
import org.pikatimer.util.WaveStringConverter;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import javafx.animation.KeyFrame;
import javafx.animation.PauseTransition;
import javafx.animation.Timeline;
import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ListChangeListener;
import javafx.collections.ListChangeListener.Change;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonBar.ButtonData;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.ContentDisplay;
import javafx.scene.control.Dialog;
import javafx.scene.control.IndexedCell;
import javafx.scene.control.Label;
import javafx.scene.control.ListCell;
import javafx.scene.control.ListView;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import static javafx.scene.control.TableView.CONSTRAINED_RESIZE_POLICY;
import javafx.scene.control.TextField;
import javafx.scene.control.cell.CheckBoxTableCell;
import javafx.scene.control.cell.ComboBoxTableCell;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.control.cell.TextFieldListCell;
import javafx.scene.control.skin.VirtualFlow;
import javafx.scene.layout.ColumnConstraints;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import static javafx.scene.layout.Region.USE_COMPUTED_SIZE;
import javafx.scene.layout.VBox;
import javafx.scene.text.Font;
import javafx.stage.Modality;
import javafx.stage.Screen;
import javafx.stage.Stage;
import javafx.util.StringConverter;
import org.apache.commons.lang3.StringUtils;
import org.controlsfx.control.CheckComboBox;
import org.controlsfx.control.PrefixSelectionComboBox;
import org.controlsfx.control.ToggleSwitch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FXML Controller class
 *
 * @author jcgarner
 */

public class FXMLTimingController {
    private static final Logger logger = LoggerFactory.getLogger(FXMLTimingController.class);

    // Real monospaced fonts with a normal stroke weight, in order of preference.
    // The JavaFX logical "Monospaced" font maps to Courier New on macOS, which
    // is thin and looks grey next to the rest of the table.
    private static final List<String> MONO_FONT_CANDIDATES = List.of(
            "Menlo", "Consolas", "DejaVu Sans Mono", "Liberation Mono", "Ubuntu Mono");
    private static String monoFontFamily;

    private static synchronized String monoFontFamily() {
        if (monoFontFamily == null) {
            List<String> installed = Font.getFamilies();
            monoFontFamily = MONO_FONT_CANDIDATES.stream()
                    .filter(installed::contains)
                    .findFirst()
                    .orElse("Monospaced");
        }
        return monoFontFamily;
    }

    private static String monoCellStylesheet() {
        String css = ".table-cell.mono-cell { -fx-font-family: \"" + monoFontFamily() + "\"; }";
        return "data:text/css;base64," + java.util.Base64.getEncoder()
                .encodeToString(css.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    @FXML private VBox timingVBox;
    @FXML private ListView<TimingLocation> timingLocListView;
    @FXML private Button timingLocRemoveAllButton;
    @FXML private Button timingLocAddButton;
    @FXML private Button timingLocRemoveButton;  
    
    @FXML private VBox timingLocVBox;
    @FXML private TextField timingLocationNameTextField; 
    
    @FXML private TextField filterStartTextField;
    @FXML private TextField filterEndTextField; 
            
    @FXML private TextField searchTextBox;
    @FXML private CheckComboBox<TimingLocation> searchScopeCheckComboBox;
    @FXML private Label listSizeLabel;
    @FXML private Label filteredSizeLabel;
    @FXML private TableView<CookedTimeData> timeTableView;
    @FXML private TableColumn<CookedTimeData,String> bibColumn;
    @FXML private TableColumn<CookedTimeData,String> chipColumn;
    @FXML private TableColumn<CookedTimeData,Duration> timeColumn;
    @FXML private TableColumn<CookedTimeData,String> nameColumn;
    @FXML private TableColumn<CookedTimeData,String> locationColumn;
    @FXML private TableColumn<CookedTimeData,String> inputColumn;
    @FXML private TableColumn<CookedTimeData,Boolean> backupColumn;
    @FXML private TableColumn<CookedTimeData,Boolean> ignoreColumn;
    @FXML private CheckBox customChipBibCheckBox;
    
    @FXML private ListView<TimeOverride> overrideListView;
//    @FXML private TableView<TimeOverride> overrideTableView;
//    @FXML private TableColumn<TimeOverride,String> overrideBibColumn;
//    @FXML private TableColumn<TimeOverride,String> overrideTimeColumn;
//    @FXML private TableColumn<TimeOverride,String> overrideSplitColumn;
//    @FXML private TableColumn<TimeOverride,Boolean> overrideRelativeColumn;
    
    @FXML private Button overrideEditButton;
    @FXML private Button overrideRemoveButton;
    
    @FXML private ToggleSwitch announcerToggleSwitch;
    @FXML private ToggleSwitch assignToRaceToggleSwitch;
    //@FXML private ChoiceBox<Race> assignToRaceChoiceBox;
    @FXML private ComboBox<Race> assignToRaceComboBox;
    
    private ObservableList<CookedTimeData> cookedTimeList;
    private BackgroundSortedFilteredList<CookedTimeData> timeView;
    private ObservableList<TimingLocation> timingLocationList;
    private TimingLocation selectedTimingLocation;
    private RaceDAO raceDAO;
    private TimingDAO timingDAO; 
    private ParticipantDAO participantDAO;
    //private FXMLTimingLocationInputController timingLocationDetailsController;
    private FXMLLoader timingLocationDetailsLoader ;
    private Bib2ChipMap bib2ChipMap;
    
    @FXML private VBox timingDetailsVBox;
    /**
     * Initializes the controller class.
     */
    @FXML
    public void initialize() {
        
        selectedTimingLocation = null;
        
        
        raceDAO=RaceDAO.getInstance();
        timingDAO=TimingDAO.getInstance();
        participantDAO=ParticipantDAO.getInstance();
        timingLocationList=timingDAO.listTimingLocations(); 
        
        timingLocListView.setItems(timingLocationList);
        timingLocListView.setEditable(true);
        
        //timingLocListView.setCellFactory(TextFieldListCell.forListView(null));
        timingLocListView.setCellFactory(TextFieldListCell.forListView(new StringConverter(){
            @Override
            public TimingLocation fromString(String s) {
                TimingLocation t = new TimingLocation();
                t.setLocationName(s);
                return t; 
            }
            @Override
            public String toString(Object t) {
                if (t != null) {
                return ((TimingLocation)t).toString(); 
                } else {
                    logger.debug("Timing StringConverter toString null object detected.");
                    return "";
                }
            }
        }        
        ));		

        timingLocListView.setOnEditCommit((ListView.EditEvent<TimingLocation> t) -> {
            logger.debug("setOnEditCommit " + t.getIndex());
            if(t.getIndex() < t.getSource().getItems().size()) {
                TimingLocation tl = t.getSource().getItems().get(t.getIndex()); 
                if (t.getNewValue().toString().isEmpty()) {
                    //timingLocationDAO.removeTimingLocation(tl);
                    //tl.setLocationName("New Timing Location");
                    timingDAO.removeTimingLocation(tl);
                } else {
                    tl.setLocationName(t.getNewValue().toString());
                    timingDAO.updateTimingLocation(tl);
                }
            } else {
                logger.debug("Timing setOnEditCommit event out of index: " + t.getIndex());
            }
            timingLocAddButton.requestFocus();
           // timingLocAddButton.setDefaultButton(true);
        });

        timingLocListView.setOnEditCancel((ListView.EditEvent<TimingLocation> t) ->{
            logger.debug("setOnEditCancel " + t.getIndex());
            if (t.getIndex() >= 0 && t.getIndex() < t.getSource().getItems().size()) {
                TimingLocation tl = t.getSource().getItems().get(t.getIndex());
                if (tl.getLocationName().isEmpty()) {
                    //tl.setLocationName("New Timing Location");
                    timingDAO.removeTimingLocation(tl);
                }
            } else {
                logger.debug("Timing setOnEditCancel event out of index: " + t.getIndex());
            }
            timingLocAddButton.requestFocus();
            //timingLocAddButton.setDefaultButton(true);
        });
        
        
        timingLocRemoveButton.disableProperty().bind(timingLocListView.getSelectionModel().selectedItemProperty().isNull());

        

        
        
        // The table can hold a million or more times, so the filtering and
        // sorting happen in the background (see BackgroundSortedFilteredList)
        // and the predicate / sort keys only use snapshots taken here on the
        // JavaFX thread. 
        cookedTimeList = timingDAO.getCookedTimes();
        timeView = new BackgroundSortedFilteredList<>(cookedTimeList, "Thread-TimingTableView", javafx.util.Duration.millis(250));
        timeView.setUpdateHooks(this::beforeTimeViewUpdate, this::afterTimeViewUpdate);

        // Set the filter Predicate whenever the filter changes.
        // We will use a Timeline to act as a debounce so the filter will only
        // fire after .25 seconds of no input. 
        Timeline debounceTimeline = new Timeline();
        javafx.util.Duration bounceTime = javafx.util.Duration.millis(250);
        searchTextBox.textProperty().addListener((observable, oldValue, newValue) -> {
            logger.trace("TimingController searchTextBox listener fired...");
            debounceTimeline.stop();
            
            // Configure the action to take once the timer finishes
            KeyFrame keyFrame = new KeyFrame(bounceTime, (event) -> {
                updateFilterPredicate();
            });

            // Refresh the timeline with the new keyframe and start the countdown
            debounceTimeline.getKeyFrames().setAll(keyFrame);
            debounceTimeline.play();
        });
        searchScopeCheckComboBox.getCheckModel().getCheckedItems().addListener((ListChangeListener.Change<? extends TimingLocation> c) -> {
            
            logger.debug("TimingPartController::searchScopeCheckComboBox(changeListener) fired...");
            updateFilterPredicate();
        });
        
        searchScopeCheckComboBox.getItems().setAll(timingLocationList);
        
        // The name filter / sort work off of a snapshot of the participants, 
        // so take a new one once the participants settle down after a change. 
        PauseTransition participantsChanged = new PauseTransition(javafx.util.Duration.seconds(1));
        participantsChanged.setOnFinished(e -> {
            if (!searchTextBox.getText().isEmpty()) updateFilterPredicate();
            if (timeTableView.getSortOrder().contains(nameColumn)) updateSort();
        });
        participantDAO.listParticipants().addListener((ListChangeListener.Change<? extends Participant> c) -> {
            if (Platform.isFxApplicationThread()) participantsChanged.playFromStart();
            else Platform.runLater(participantsChanged::playFromStart);
        });

        // The table does not sort its items itself, it hands the sort order 
        // to the background list. 
        timeTableView.setSortPolicy(tv -> {
            updateSort();
            return true;
        });
        
        // Set the cell factories and stort routines... 
        // Bib, Chip, and Time use a fixed width font. The mono-cell class only
        // matches the table cells, so the column headers keep the normal font.
        // The family is picked at runtime (see monoFontFamily()) because JavaFX
        // CSS has no font fallback list.
        timeTableView.getStylesheets().add(monoCellStylesheet());
        bibColumn.setCellValueFactory(c -> c.getValue().bibProperty());
        bibColumn.setStyle( "-fx-alignment: CENTER-RIGHT;");
        bibColumn.getStyleClass().add("mono-cell");
        
        chipColumn.setCellValueFactory(c -> c.getValue().rawChipIDProperty());
        chipColumn.setStyle( "-fx-alignment: CENTER-RIGHT;");
        chipColumn.getStyleClass().add("mono-cell");
        
        // Only turn the Duration into text for the cells that are on the screen. 
        timeColumn.setCellValueFactory(c -> c.getValue().timestampProperty());
        timeColumn.setCellFactory(column -> new TableCell<CookedTimeData,Duration>() {
            @Override
            protected void updateItem(Duration d, boolean empty) {
                super.updateItem(d, empty);
                if (d == null || empty) setText(null);
                else setText(DurationFormatter.durationToString(d, 3));
            }
        });
        timeColumn.setStyle( "-fx-alignment: CENTER-RIGHT;");
        timeColumn.getStyleClass().add("mono-cell");
        
        nameColumn.setCellValueFactory(cellData -> {
            String bib = cellData.getValue().getBib();
            Participant p = participantDAO.getParticipantByBib(bib);
            if (p == null) { 
                return new SimpleStringProperty(unregisteredName(cellData.getValue()));
            } else {
                return p.fullNameProperty();
            }
        });
        locationColumn.setCellValueFactory(cellData -> {
            TimingLocation t =  timingDAO.getTimingLocationByID(cellData.getValue().getTimingLocationId());
            if (t == null) { return new SimpleStringProperty("Unknown");
            } else {
                return t.LocationNameProperty();
            }
        });
        inputColumn.setCellValueFactory(cellData -> {
            TimingLocation t =  timingDAO.getTimingLocationByID(cellData.getValue().getTimingLocationId());
            
            if (t == null) return new SimpleStringProperty("Unknown");
            else {
                TimingLocationInput tl = t.getInputByID(cellData.getValue().getTimingLocationInputId());
                if (tl == null) return new SimpleStringProperty("Unknown");
                return tl.LocationNameProperty();
            }
        });
        
        backupColumn.setCellValueFactory(c -> c.getValue().backupTimeProperty());
        backupColumn.setCellFactory(column -> new BackupFlagTableCell());
        backupColumn.setEditable(false);
        backupColumn.setStyle( "-fx-alignment: CENTER;");
        
        // The cell saves the change itself (the cooked time list has no 
        // extractor to tell anyone that the flag changed)
        ignoreColumn.setCellValueFactory(c -> c.getValue().ignoreTimeProperty());
        ignoreColumn.setCellFactory(tc -> new IgnoreFlagTableCell());
        
        // Add the sorted (and filtered) data to the table.
        timeTableView.setItems(timeView);
        timeTableView.setPlaceholder(new Label("No times have been entered yet"));
        
        // set the default sort order to the finish time
        timeColumn.setSortType(TableColumn.SortType.DESCENDING);
        timeTableView.getSortOrder().clear();
        timeTableView.getSortOrder().add(timeColumn);
        
        listSizeLabel.textProperty().bind(Bindings.size(cookedTimeList).asString());
        filteredSizeLabel.textProperty().bind(Bindings.size(timeView).asString());
        
        
        // load up the TimingLocationDetailsPane
                    
        //if there are no timing locations selected in the view then disable the entire right hand side
        timingDetailsVBox.visibleProperty().bind(timingLocListView.getSelectionModel().selectedItemProperty().isNull().not());
         
        
        assignToRaceComboBox.disableProperty().bind(assignToRaceToggleSwitch.selectedProperty().not());
        assignToRaceComboBox.visibleProperty().bind(Bindings.size(RaceDAO.getInstance().listWaves()).greaterThan(1));
        assignToRaceComboBox.managedProperty().bind(Bindings.size(RaceDAO.getInstance().listWaves()).greaterThan(1));
        assignToRaceToggleSwitch.visibleProperty().bind(Bindings.size(RaceDAO.getInstance().listWaves()).greaterThan(1));
        assignToRaceToggleSwitch.managedProperty().bind(Bindings.size(RaceDAO.getInstance().listWaves()).greaterThan(1));
        
        timingLocListView.getSelectionModel().getSelectedItems().addListener((ListChangeListener.Cha‌​nge<? extends TimingLocation> c) -> { 
            logger.debug("timingLocListView changed...");
            //timingLocListView.getSelectionModel().getSelectedItems().forEach(System.out::println); 
            ObservableList<TimingLocation> selectedTimingLocations = timingLocListView.getSelectionModel().getSelectedItems();

            timingDetailsVBox.getChildren().clear();

            if ( selectedTimingLocations.isEmpty() ) {
               logger.debug("Nothing Selected");
               //timingLocationDetailsController.selectTimingLocation(null);
               if (selectedTimingLocation != null) {
                   timingLocationNameTextField.textProperty().unbindBidirectional(selectedTimingLocation.LocationNameProperty());
                   selectedTimingLocation=null; 
                   timingLocVBox.disableProperty().setValue(true);
               }
            } else {
                logger.debug("We just selected " + selectedTimingLocations.get(0).getLocationName());
                //timingLocationNameTextField.textProperty().setValue(selectedTimingLocations.get(0).LocationNameProperty().getValue());
                timingLocVBox.disableProperty().setValue(false);
                if (selectedTimingLocation != null) {
                    logger.debug("Unbinding timingLocationNameTextField");
                    timingLocationNameTextField.textProperty().unbindBidirectional(selectedTimingLocation.LocationNameProperty());
                }
                selectedTimingLocation=selectedTimingLocations.get(0); 
                timingLocationNameTextField.textProperty().bindBidirectional(selectedTimingLocation.LocationNameProperty());
                
                if (selectedTimingLocation.getAutoAssignRaceID() < 0) {
                    assignToRaceToggleSwitch.setSelected(false);
                    assignToRaceComboBox.getSelectionModel().clearSelection();
                } else {
                    assignToRaceToggleSwitch.setSelected(true);
                    assignToRaceComboBox.getSelectionModel().select(selectedTimingLocation.autoAssignRaceProperty().getValue());
                }
                
                if (selectedTimingLocation.getIsAnnouncer()) announcerToggleSwitch.setSelected(true);
                else announcerToggleSwitch.setSelected(false);
                
                // Show the filter start/end values
                filterEndTextField.textProperty().setValue(DurationFormatter.durationToString(selectedTimingLocation.getFilterEndDuration(), 3, Boolean.TRUE).replace(".000", ""));
                filterStartTextField.textProperty().setValue(DurationFormatter.durationToString(selectedTimingLocation.getFilterStartDuration(), 3, Boolean.TRUE).replace(".000", ""));
                
                logger.debug("Selected timing location is now " + selectedTimingLocation.getLocationName());
                //timingLocationDetailsController.setTimingLocationInput(null); // .selectTimingLocation(selectedTimingLocations.get(0));
                if (selectedTimingLocation.inputsProperty().isEmpty() ) { // no inputs yet
                    addTimingInput(null);
                } else { // display all of the inputs
                    logger.debug("Starting the display of inputs for a timing location");
                    // Only open the input up if it is the only one
                    boolean expand = selectedTimingLocation.getInputs().size() == 1;
                    selectedTimingLocation.getInputs().forEach(i -> {
                        logger.debug("showing input for a timing location ");
                        showTimingInput(i, expand);
                    });
                    logger.debug("Done showing inputs for a timing location");
                }
            }
        });
         
       
        
        timingLocationNameTextField.focusedProperty().addListener((ObservableValue<? extends Boolean> arg0, Boolean oldPropertyValue, Boolean newPropertyValue) -> {
            if (!newPropertyValue) {
            logger.debug("timingLocationNameTextField out focus");

                if ( timingLocationNameTextField.getText().isEmpty() ) {
                    timingLocationNameTextField.textProperty().setValue("Unnamed");
                    timingDAO.updateTimingLocation(timingLocListView.getSelectionModel().getSelectedItems().get(0));
                }
                
                timingDAO.updateTimingLocation(timingLocListView.getSelectionModel().getSelectedItems().get(0));
                
            }
        });
        
        // Use this if you whant keystroke by keystroke monitoring.... Reject any non digit attempts
//        filterStartTextField.textProperty().addListener((observable, oldValue, newValue) -> {
//            if ( newValue.isEmpty() || newValue.matches("([0-9]*|[0-9]*:[0-5]?)") ) {
//                logger.debug("Possiblely good filter Time (newValue: " + newValue + ")");
//            } else if(newValue.matches("[0-9]*:[0-5][0-9]") ) { // Looks like a HH:MM time, lets check
//                logger.debug("Looks like a valid start filter Time (newValue: " + newValue + ")");
//            } else {
//                filterStartTextField.setText(oldValue);
//                logger.debug("Bad filter Start Time (newValue: " + newValue + ")");
//            }
//                
//        });
        // Times of day (or elapsed from midnight), saved when the textfield focus changes. 
        TemporalField<Duration> filterStartField = TextFieldFormatters.durationField(filterStartTextField, "H:MM[:ss[.SSS]]");
        TemporalField<Duration> filterEndField = TextFieldFormatters.durationField(filterEndTextField, "H:MM[:ss[.SSS]]");
        filterStartTextField.focusedProperty().addListener((ObservableValue<? extends Boolean> arg0, Boolean oldPropertyValue, Boolean newPropertyValue) -> {
            if (!newPropertyValue) {
                logger.debug("filterStartTextField out focus");
                Duration newTime;
                if (filterStartField.isValid()){
                    newTime = filterStartField.getValue() == null ? Duration.ZERO : filterStartField.getValue();
                } else {
                    filterStartTextField.textProperty().setValue(DurationFormatter.durationToString(selectedTimingLocation.getFilterStartDuration(), 3, Boolean.TRUE).replace(".000", ""));
                    return;
                }
                
                if (selectedTimingLocation.getFilterStartDuration().equals(newTime)) {
                    filterStartTextField.textProperty().setValue(DurationFormatter.durationToString(selectedTimingLocation.getFilterStartDuration(), 3, Boolean.TRUE).replace(".000", ""));
                    return; 
                } else if ( filterStartTextField.getText().isEmpty() ) {
                    // set duration to zero
                    selectedTimingLocation.setFilterStart(0L);
                } else {
                    // duration is not zero... parse it
                    selectedTimingLocation.setFilterStart(newTime.toNanos());
                }
                timingDAO.updateTimingLocation(timingLocListView.getSelectionModel().getSelectedItems().get(0));
                filterStartTextField.textProperty().setValue(DurationFormatter.durationToString(selectedTimingLocation.getFilterStartDuration(), 3, Boolean.TRUE).replace(".000", ""));
                selectedTimingLocation.reprocessReads();
            }
        });
        filterEndTextField.focusedProperty().addListener((ObservableValue<? extends Boolean> arg0, Boolean oldPropertyValue, Boolean newPropertyValue) -> {
            if (!newPropertyValue) {
                logger.debug("filterEndTextField out focus");
                                        
                Duration newTime;
                if (filterEndField.isValid()){
                    newTime = filterEndField.getValue() == null ? Duration.ZERO : filterEndField.getValue();
                } else {
                    filterEndTextField.textProperty().setValue(DurationFormatter.durationToString(selectedTimingLocation.getFilterEndDuration(), 3, Boolean.TRUE).replace(".000", ""));
                    return;
                }
                
                if (selectedTimingLocation.getFilterEndDuration().equals(newTime)) {
                    filterEndTextField.textProperty().setValue(DurationFormatter.durationToString(selectedTimingLocation.getFilterEndDuration(), 3, Boolean.TRUE).replace(".000", ""));
                    return; 
                } else if ( filterEndTextField.getText().isEmpty() ) {
                    // set duration to zero
                    selectedTimingLocation.setFilterEnd(0L);
                } else {
                    // duration is not zero... parse it
                    selectedTimingLocation.setFilterEnd(newTime.toNanos());
                }
                timingDAO.updateTimingLocation(timingLocListView.getSelectionModel().getSelectedItems().get(0));
                filterEndTextField.textProperty().setValue(DurationFormatter.durationToString(selectedTimingLocation.getFilterEndDuration(), 3, Boolean.TRUE).replace(".000", ""));
                selectedTimingLocation.reprocessReads();
            }
        });
        
        announcerToggleSwitch.selectedProperty().addListener((ObservableValue<? extends Boolean> arg0, Boolean oldPropertyValue, Boolean newPropertyValue) -> {
            TimingLocation tl = timingLocListView.getSelectionModel().getSelectedItems().get(0);
            if (tl != null && !Objects.equals(newPropertyValue, tl.getIsAnnouncer())) {
                tl.setIsAnnouncer(newPropertyValue);
                timingDAO.updateTimingLocation(tl);
            }
        });
        
        assignToRaceToggleSwitch.selectedProperty().addListener((ObservableValue<? extends Boolean> arg0, Boolean oldPropertyValue, Boolean newPropertyValue) -> {
            if (!newPropertyValue) { // Don't auto-assign
                TimingLocation tl = timingLocListView.getSelectionModel().getSelectedItems().get(0);
                if (tl != null) {
                    if (!tl.getAutoAssignRaceID().equals(-1)) {
                        logger.debug("Everybody at " + tl.getLocationName() + " is no longer auto-assigned");
                        tl.setAutoAssignRaceID(-1);
                        timingDAO.updateTimingLocation(tl);
                        assignToRaceComboBox.getSelectionModel().clearSelection();
                        //selectedTimingLocation.reprocessReads();
                    }
                }
            } else {
                // Do nothing. If they select a race to assign the runners to then we will do something.
            }
        });
        assignToRaceComboBox.setItems(RaceDAO.getInstance().listRaces());
        assignToRaceComboBox.focusedProperty().addListener((ObservableValue<? extends Boolean> arg0, Boolean oldPropertyValue, Boolean newPropertyValue) -> {
            if (!newPropertyValue) {
                TimingLocation tl = timingLocListView.getSelectionModel().getSelectedItems().get(0);
                Race race = assignToRaceComboBox.getSelectionModel().getSelectedItem();
                if (tl != null && race != null) {
                    if (!tl.getAutoAssignRaceID().equals(race.getID())) {
                        logger.debug("Everybody at " + tl.getLocationName() + " now assigned to race " + race.getRaceName());
                        tl.setAutoAssignRaceID(race.getID());
                        timingDAO.updateTimingLocation(tl);
                        selectedTimingLocation.reprocessReads();
                    }
                }
            }
        });

        
        // bib2Chip mappings
        bib2ChipMap = timingDAO.getBib2ChipMap();
        customChipBibCheckBox.setSelected(bib2ChipMap.getUseCustomMap()); 
        //customChipBibCheckBox.selectedProperty().bind(bib2ChipMap.useCustomMapProperty());
        
        customChipBibCheckBox.setOnAction(a -> {
            logger.debug("bib2ChipMap custom checkbox clicked!");
            if (bib2ChipMap.useCustomMapProperty().get()) {
                bib2ChipMap.setUseCustomMap(false);
                timingDAO.saveBib2ChipMap(bib2ChipMap);
                timingDAO.reprocessAllRawTimes();
            } else if (!bib2ChipMap.useCustomMapProperty().get() && !bib2ChipMap.getChip2BibMap().isEmpty() ) {
                bib2ChipMap.setUseCustomMap(true);
                timingDAO.saveBib2ChipMap(bib2ChipMap);
                timingDAO.reprocessAllRawTimes();
            } else {
                setupCustomChipMap(null);
            }
        });    
//        customChipBibCheckBox.selectedProperty().addListener((ObservableValue<? extends Boolean> ov, Boolean old_val, Boolean new_val) -> {
//            if (!old_val.equals(new_val)) {
//                logger.debug("Setting bib2ChipMap custom to " + new_val.toString());
//                if (new_val) setupCustomChipMap(null);
//                //bib2ChipMap.setUseCustomMap(new_val);
//                //timingDAO.saveBib2ChipMap(bib2ChipMap);
//            }
//        });
        
        
        // Override table
        overrideListView.setPlaceholder(new Label("No overrides have been entered yet"));
        overrideListView.setItems(timingDAO.getOverrides());
        overrideListView.setCellFactory(param -> new ListCell<TimeOverride>() {
            Label locationLabel = new Label("Location: ");
            Label bonusLabel = new Label("Bonus: ");
            Label penaltyLabel = new Label("Penalty: ");
            Label bib = new Label("");
            Label fullName = new Label("");
            Label split = new Label("");
            Label time = new Label("");
            Label note = new Label("");
            VBox toVBox = new VBox();
            HBox nameHBox = new HBox();
            HBox timeHBox = new HBox();
            
            @Override
            protected void updateItem(TimeOverride to, boolean empty) {
                super.updateItem(to, empty);
                
                if (empty || to == null || to.getBib() == null) {
                    setText(null);
                    setGraphic(null);
                } else {
                    
                    bib.setText("Bib: " + to.getBib());
                    fullName.textProperty().bind(ParticipantDAO.getInstance().getParticipantByBib(to.getBib()).fullNameProperty());
                    nameHBox.setSpacing(5);
                    nameHBox.getChildren().setAll(bib,fullName);
                    nameHBox.setStyle("-fx-font-size: 14px;");
                    
                    if (null == to.getOverrideType()) {
                        
                    } else switch (to.getOverrideType()) {
                        case OVERRIDE:
                            split.textProperty().bind(raceDAO.getSplitByID(to.getSplitId()).splitNameProperty());
                            time.setText(DurationFormatter.durationToString(to.getTimestamp()));
                            timeHBox.getChildren().setAll(locationLabel,split,time);
                            break;
                        case PENALTY:
                            time.setText(DurationFormatter.durationToString(to.getTimestamp()));
                            timeHBox.getChildren().setAll(penaltyLabel,time);
                            break;
                        case BONUS:
                            time.setText(DurationFormatter.durationToString(to.getTimestamp()));
                            timeHBox.getChildren().setAll(bonusLabel,time);
                        default:
                            break;
                    }
                    timeHBox.setSpacing(5);
                    
                    if (to.getNote().isEmpty()) toVBox.getChildren().setAll(nameHBox,timeHBox);
                    else {
                        note.setText(to.getNote());
                        toVBox.getChildren().setAll(nameHBox,timeHBox,note);
                    }
                    
                    setText(null);
                    setGraphic(toVBox);
                    
                    setOnMouseClicked(ev -> {
                        if (ev.getClickCount() == 2 )
                            editOverride(getItem());
                    });
                }
            }
        });
        
        
//        overrideTableView.setPlaceholder(new Label("No overrides have been entered yet"));
//        overrideTableView.setItems(timingDAO.getOverrides());
//        overrideBibColumn.setCellValueFactory(cellData -> {
//            return cellData.getValue().bibProperty();
//        });
//        overrideBibColumn.setComparator(new AlphanumericComparator());
//                
//        overrideSplitColumn.setCellValueFactory(cellData -> {
//            Split s = raceDAO.getSplitByID(cellData.getValue().getSplitId());
//            if (s== null) { return new SimpleStringProperty("Unknown: " + cellData.getValue().getSplitId());
//            } else {
//                return s.splitNameProperty();
//            }
//        });
//        
//        
//        overrideTimeColumn.setCellValueFactory(cellData -> {
//            return cellData.getValue().timestampStringProperty();
//            //return new SimpleStringProperty(DurationFormatter.durationToString(cellData.getValue().getTimestamp(), 3));
//        });
//        
//        overrideRelativeColumn.setCellValueFactory(cellData -> {
//            return cellData.getValue().relativeProperty();
//        });
//        overrideRelativeColumn.setCellFactory(tc -> new CheckBoxTableCell());
//        
//        overrideTableView.setRowFactory((TableView<TimeOverride> tableView1) -> {
//            final TableRow<TimeOverride> row = new TableRow<>();
//
//            row.setOnMouseClicked(event -> {
//            if (event.getClickCount() == 2 && (! row.isEmpty()) ) {
//                editOverride(overrideTableView.getSelectionModel().getSelectedItem());
//            }
//            });
//        
//        
//            return row;
//        
//        });
        
        overrideEditButton.disableProperty().bind(overrideListView.getSelectionModel().selectedItemProperty().isNull());
        overrideRemoveButton.disableProperty().bind(overrideListView.getSelectionModel().selectedItemProperty().isNull());
        
         timingLocListView.getSelectionModel().clearAndSelect(0);
         
         timingLocationList.addListener((Change<? extends TimingLocation> change) -> {
            //waveComboBox.getItems().clear();
            //Platform.runLater(() -> {
            logger.debug("TimingController::timingLocationList(changeListener) fired...");
                
            // TODO
            //rework the popup menu for the add/delete
            searchScopeCheckComboBox.getCheckModel().clearChecks();
//            while (change.next() ) 
//                change.getRemoved().forEach(removed -> {
//                    searchScopeCheckComboBox.getCheckModel().clearCheck(removed);
//                });
            searchScopeCheckComboBox.getItems().setAll(timingLocationList);
            

            //});
        });
    }    
    
    
        public void resetTimingLocations(ActionEvent fxevent){
        // prompt 
        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Confirm Resetting all Timing Locations");
        alert.setHeaderText("This action cannot be undone.");
        alert.setContentText("This will reset the timing locations to default values.\nAll splits will be reassigned to one of the default locations.");
        //Label alertContent = new Label("This will reset the timing locations to default values.\nAll splits will be reassigned to one of the default locations.");
        //alertContent.setWrapText(true); 
        //alert.getDialogPane().setContent(alertContent);
        
        Optional<ButtonType> result = alert.showAndWait();
        if (result.get() == ButtonType.OK){
            timingDAO.createDefaultTimingLocations();
        } else {
            // ... user chose CANCEL or closed the dialog
        }
        
        timingLocAddButton.requestFocus();
        //timingLocAddButton.setDefaultButton(true);
    }
        
    public void addTimingLocation(ActionEvent fxevent){
        // prompt 
        TimingLocation t = new TimingLocation();
        t.setLocationName("New Timing Location");

        
        timingDAO.addTimingLocation(t);

        logger.debug("Setting the timingLocListView.edit to " + timingLocationList.size() + " " + timingLocationList.indexOf(t));
        timingLocListView.getSelectionModel().select(timingLocationList.indexOf(t));
        
        //timingLocListView.edit(timingLocationList.indexOf(t));
        timingLocationNameTextField.requestFocus();
        timingLocationNameTextField.selectAll();
        //Because we call the timingLocListView.edit, we don't want to pull back focus
        //timingLocAddButton.requestFocus();

    }
    public void removeTimingLocation(ActionEvent fxevent){
        final TimingLocation tl = timingLocListView.getSelectionModel().getSelectedItem();
        
        // If the location is referenced by a split, 
        // toss up a warning and leave it alone
        final StringProperty splitsUsing = new SimpleStringProperty();
        raceDAO.listRaces().forEach(r -> {
            r.getSplits().forEach(s -> {
                if (s.getTimingLocation().equals(tl)) splitsUsing.set(splitsUsing.getValueSafe() + r.getRaceName() + " " + s.getSplitName() + "\n");
            });
        });
        
        if (splitsUsing.isEmpty().get()) {
            timingDAO.removeTimingLocation(tl);;
            timingLocAddButton.requestFocus();
            //timingLocAddButton.setDefaultButton(true);
        } else {
            Alert alert = new Alert(AlertType.INFORMATION);
            alert.setTitle("Unable to Remove Timing Location");
            alert.setHeaderText("Unable to remove the " + tl.getLocationName() + " timing location.");
            alert.setContentText("The timing location is in use by the following splits:\n" + splitsUsing.getValueSafe());
            alert.showAndWait();
        }
    }
    
    public void addTimingInput(ActionEvent fxevent){
        TimingLocationInput tli = new TimingLocationInput();
        tli.setTimingLocation(selectedTimingLocation);
        tli.setLocationName("New " + selectedTimingLocation.getLocationName() + " Input " + (selectedTimingLocation.inputsProperty().size()+1));
        timingDAO.addTimingLocationInput(tli);
        showTimingInput(tli, true);
        //timingLocationDetailsController.selectTimingLocation(selectedTimingLocation);
    }
    
    private void showTimingInput(TimingLocationInput i, boolean expanded) {
        logger.debug("showTimingInput called... ");
        FXMLLoader tlLoader = new FXMLLoader(getClass().getResource("/org/pikatimer/timing/FXMLTimingLocationInput.fxml"));
        try {
            timingDetailsVBox.getChildren().add(tlLoader.load());
            
        } catch (IOException ex) {
            logger.error("Unable to load FXML FXMLTimingLocationInput.fxml",ex);
        }
        FXMLTimingLocationInputController tliController = tlLoader.getController();
        tliController.setTimingLocationInput(i); 
        tliController.setExpanded(expanded);
        //timingLocationDetailsController.selectTimingLocation(selectedTimingLocation);
    }
    
    public void clearAllTimes(ActionEvent fxevent){
        //TODO: Prompt and then remove all times associated with that timing location 
        // _or_ all timing locations
        Alert alert = new Alert(AlertType.CONFIRMATION);
        alert.setTitle("Clear Timing Data...");
        alert.setHeaderText("Clear Timing Data:");
        alert.setContentText("Do you want to clear the times for all locations or just the " + selectedTimingLocation.getLocationName()+ " location?");

        ButtonType allButtonType = new ButtonType("All Times");
        
        ButtonType currentButtonType = new ButtonType(selectedTimingLocation.getLocationName() + " Times",ButtonData.YES);
        ButtonType cancelButtonType = new ButtonType("Cancel", ButtonData.CANCEL_CLOSE);

        alert.getButtonTypes().setAll(cancelButtonType, allButtonType,  currentButtonType );

        Optional<ButtonType> result = alert.showAndWait();
        if (result.get() == allButtonType){
            // ... user chose All
            timingDAO.clearAllTimes();
            // itterate over all of the timing locations
            timingLocationList.stream().forEach(tl -> {tl.getInputs().stream().forEach(tli -> {tli.clearLocalReads();});});
        } else if (result.get() == currentButtonType) {
            // ... user chose "Two"
            selectedTimingLocation.getInputs().stream().forEach(tli -> {tli.clearReads();});
        } else {
            // ... user chose CANCEL or closed the dialog
        }
 
    }

    public void setupCustomChipMap(ActionEvent fxevent){
        
        
        FXMLLoader fxmlLoader = new FXMLLoader(getClass().getResource("FXMLSetupBibMap.fxml"));
        Parent chipMapRoot;
        try {
            
            chipMapRoot = (Parent) fxmlLoader.load();
            Stage stage = new Stage();
            stage.initModality(Modality.APPLICATION_MODAL);
            stage.setTitle("Chip to Bib Map Setup");
            stage.setScene(new Scene(chipMapRoot));  
            stage.showAndWait();
        } catch (IOException ex) {
            logger.warn("Error in setupCustomChipMap:",ex);
        }
            
        customChipBibCheckBox.setSelected(bib2ChipMap.getUseCustomMap()); 
    }
    
    public void addOverride(ActionEvent fxevent){
        editOverride(new TimeOverride());
    }
    
    public void editOverride(ActionEvent fxevent){
        editOverride(overrideListView.getSelectionModel().getSelectedItem());
    }
    
    public void editOverride(TimeOverride o){
        // Open a Dialog box and ask for four things:
        // 1) Bib
        // 2) Type (override, penalty, bonus)
        // 3) split (based on the bib)
        // 4) Time
        // 5) if that time is based on the time of day or a duration from the start
        // 
        // The dialog modifies a timeOverride object on the fly....
        if (o == null) return;
        
        
        BooleanProperty bibOK = new SimpleBooleanProperty(false);
        BooleanProperty timeOK = new SimpleBooleanProperty(false);
        BooleanProperty splitOK = new SimpleBooleanProperty(false);
        BooleanProperty allOK = new SimpleBooleanProperty(false);
       
        allOK.bind(Bindings.and(bibOK, timeOK));
        // Create the custom dialog.
        Dialog<TimeOverride> dialog = new Dialog();
        if (o.getID() == null) {
            dialog.setTitle("Add Override");
            dialog.setHeaderText("Add Override");
        } else {
            dialog.setTitle("Edit Override");
            dialog.setHeaderText("Edit Override");
        }
        // Set the button types.
        ButtonType createButtonType;
        
        if (o.getID() == null) {
            createButtonType = new ButtonType("Create", ButtonData.OK_DONE);
        } else {
            createButtonType = new ButtonType("Save", ButtonData.OK_DONE);
        }
        
        dialog.getDialogPane().getButtonTypes().addAll(createButtonType, ButtonType.CANCEL);

        // Create the various labels and fields.
        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(10);
        grid.setPadding(new Insets(20, 20, 10, 10));
        ColumnConstraints column = new ColumnConstraints(USE_COMPUTED_SIZE);
        grid.getColumnConstraints().add(column);

        TextField bibTextField = new TextField();
        bibTextField.setPromptText("Bib");
        
        bibTextField.setPrefWidth(50.0);
        Label participantLabel = new Label();
        PrefixSelectionComboBox<TimeOverrideType> typeComboBox = new PrefixSelectionComboBox();
        PrefixSelectionComboBox<Split> splitComboBox = new PrefixSelectionComboBox();
        
        Label splitLabel = new Label("Split:");
        
        typeComboBox.setItems(FXCollections.observableArrayList(TimeOverrideType.values()));
        
        
        splitComboBox.setVisibleRowCount(5);
        bibTextField.textProperty().addListener((observable, oldValue, newValue) -> {
            Participant p = participantDAO.getParticipantByBib(newValue);
            if (p != null) {
                participantLabel.setText(p.fullNameProperty().getValueSafe());
                ObservableList<Split> splitList = FXCollections.observableArrayList();
                p.wavesObservableList().forEach(w -> {
                    splitList.addAll(w.getRace().getSplits());
                });
                
                if (p.wavesObservableList().size() > 1) splitComboBox.setConverter(
                    new StringConverter<Split>() {
                        @Override
                        public String toString(Split s) {
                            if (s == null) {
                                return "";
                            } else {
                                return s.getRace().getRaceName() + ": " + s.getSplitName();
                            }
                        }

                        @Override
                        public Split fromString(String s) {
                            return null;
                        }
                    }
                );
                else splitComboBox.setConverter(null);
                
                splitComboBox.setItems(splitList);
                splitComboBox.getSelectionModel().selectFirst();
                bibOK.set(true);
            } else {
                participantLabel.setText("Unknown");
                splitComboBox.setItems(null);
                bibOK.set(false);
            }
        });
        
        
        TextField noteTextField = new TextField(o.getNote());
        
        
        CheckBox relativeCheckBox = new CheckBox("Relative to start time");
        relativeCheckBox.selectedProperty().setValue(Boolean.FALSE);
        relativeCheckBox.disableProperty().setValue(Boolean.TRUE);
        
        TextField timeTextField = new TextField();
        // An override at a time of day needs the hours, anything relative to the start does not
        String timeOfDayPattern = "H:MM[:ss[.SSS]]";
        String elapsedPattern = "[H:]MM:ss[.SSS]";
        TemporalField<Duration> timeField = TextFieldFormatters.durationField(timeTextField, elapsedPattern);
        timeField.setRequired(true);
        Runnable updateTimePattern = () -> timeField.setPattern(
                TimeOverrideType.OVERRIDE.equals(typeComboBox.getValue()) && !relativeCheckBox.isSelected() ? timeOfDayPattern : elapsedPattern);
        relativeCheckBox.selectedProperty().addListener((obs, ov, nv) -> updateTimePattern.run());
        typeComboBox.valueProperty().addListener((obs, ov, nv) -> updateTimePattern.run());
        timeOK.bind(timeField.validProperty());
        
        
        
        

        splitComboBox.valueProperty().addListener((observable, oldValue, newValue) -> {
            if (splitComboBox.getSelectionModel().getSelectedItem() != null && splitComboBox.getSelectionModel().getSelectedItem().getPosition() == 1) {
                relativeCheckBox.selectedProperty().setValue(Boolean.FALSE);
                relativeCheckBox.disableProperty().setValue(Boolean.TRUE);
            } else {
                relativeCheckBox.disableProperty().setValue(Boolean.FALSE);
            }
        });
        
        typeComboBox.valueProperty().addListener((observable, oldValue, newValue) -> {
            if (TimeOverrideType.OVERRIDE.equals(newValue)){
                splitComboBox.setVisible(true);
                splitComboBox.setManaged(true);
                splitLabel.setVisible(true);
                splitLabel.setManaged(true);
                relativeCheckBox.setVisible(true);
                relativeCheckBox.setManaged(true);
            } else {
                splitComboBox.setVisible(false);
                splitComboBox.setManaged(false);
                splitLabel.setVisible(false);
                splitLabel.setManaged(false);
                relativeCheckBox.setVisible(false);
                relativeCheckBox.setManaged(false);
            }
            dialog.getDialogPane().getScene().getWindow().sizeToScene();
        });
        typeComboBox.getSelectionModel().select(o.getOverrideType());

                
        HBox bibLabelHBox = new HBox();
        bibLabelHBox.setSpacing(5.0);
        bibLabelHBox.getChildren().addAll(bibTextField,participantLabel);
        bibLabelHBox.setAlignment(Pos.CENTER_LEFT);
        grid.add(new Label("Bib:"), 0, 0);
        grid.add(bibLabelHBox, 1, 0);
        
        grid.add(new Label("Type"), 0, 1);
        grid.add(typeComboBox,1,1);
        
        grid.add(splitLabel, 0, 2);
        grid.add(splitComboBox, 1, 2);
        
        grid.add(new Label("Time:"),0,3);
        grid.add(timeTextField,1,3);
        //grid.add(new Label("Time Since:"),0,3);
        
        grid.add(relativeCheckBox,1,4);
        
        grid.add(new Label("Note:"),0,5);
        grid.add(noteTextField,1,5);

        // Disable create button unless everything is ok
        Node createButton = dialog.getDialogPane().lookupButton(createButtonType);
        createButton.disableProperty().bind(allOK.not());

        //grid.setMinSize(USE_COMPUTED_SIZE, USE_COMPUTED_SIZE);
        dialog.getDialogPane().setContent(grid);
        
        if (o.getID() != null) {
            Platform.runLater(() -> { 
                bibTextField.setText(o.getBib());
                bibTextField.setEditable(false); 
                bibLabelHBox.getChildren().setAll(new Label(o.getBib()), participantLabel);
                timeTextField.setText(DurationFormatter.durationToString(o.getTimestamp(), 3, Boolean.TRUE).replaceFirst("^(\\d:)", "0$1"));
                try{
                    ObservableList<Split> splitList = FXCollections.observableArrayList();
                    splitList.addAll(splitComboBox.getItems().stream().filter(e -> e.getID().equals(o.getSplitId())).findFirst().get());
                    splitComboBox.setItems(splitList);
                    splitComboBox.getSelectionModel().selectFirst();
                } catch (Exception e) {}
                
                relativeCheckBox.setSelected(o.getRelative());
                
                bibTextField.setEditable(false);
                splitComboBox.setEditable(false);
                
            });
        }

        // Convert the result to a TimeOverride
        dialog.setResultConverter(dialogButton -> {
            if (dialogButton == createButtonType) {
                o.setBib( bibTextField.textProperty().getValueSafe());
                
                if (TimeOverrideType.OVERRIDE.equals(typeComboBox.getValue())) {
                    o.setOverrideType(typeComboBox.getValue());
                    o.setSplitId(splitComboBox.getValue().getID());

                    o.setTimestamp(timeField.getValue());
                    o.setRelative(relativeCheckBox.isSelected());
                    o.setNote(noteTextField.getText());
                    
                } else {
                    o.setOverrideType(typeComboBox.getValue());
                    o.setSplitId(-1);
                    o.setTimestamp(timeField.getValue());
                    o.setRelative(true);
                    o.setNote(noteTextField.getText());
                    
                }
                return o;
            }
            return null;
        });
        // Request focus on the username field by default.
        Platform.runLater(() -> bibTextField.requestFocus());

        // Show the dialog
        Optional<TimeOverride> result = dialog.showAndWait();
        
        // If we got a time, let's save it.
        if (result.isPresent()) timingDAO.saveOverride(result.get());
        
        
    }
    
    public void removeOverride(ActionEvent fxevent){
        timingDAO.deleteOverride(overrideListView.getSelectionModel().getSelectedItem());
    }
    
    
    public void clearAllOverrides(ActionEvent fxevent){
        //TODO: Prompt and then remove all times associated with that timing location 
        // _or_ all timing locations
        Alert alert = new Alert(AlertType.CONFIRMATION);
        alert.setTitle("Clear Overrides...");
        alert.setHeaderText("Delete Overrides:");
        alert.setContentText("Do you want to delete all overrides?");

        //ButtonType allButtonType = new ButtonType("All");
        
        ButtonType deleteButtonType = new ButtonType("Delete",ButtonData.YES);
        ButtonType cancelButtonType = new ButtonType("Cancel", ButtonData.CANCEL_CLOSE);

        alert.getButtonTypes().setAll(cancelButtonType, deleteButtonType );

        Optional<ButtonType> result = alert.showAndWait();
        if (result.get() == deleteButtonType) {
            // delete all
            timingDAO.clearAllOverrides(); 
        }
 
    }
    
    public void startTriggerLookup(ActionEvent fxevent){
        
        // Check to see if we have any START_TIMEs to map
        // Wrap it in a new list to avoid the list being modified while we stream/filter it. 
        List<CookedTimeData> cTimes = new ArrayList(timingDAO.getCookedTimes()); 
        List<CookedTimeData> cookedTimes = cTimes.stream().filter(c -> !c.ignoreTimeProperty().getValue()).collect(Collectors.toList());
        
        // For each timing location look for Zero chips at that split
        Map<Integer,ObservableList<Duration>> startTimesByLocation = new HashMap();
        timingDAO.listTimingLocations().forEach(tl -> {
            Integer tlID = tl.getID();
            List<Duration> startTimes = cookedTimes.stream().filter(p -> p.getRawChipID().equals("0") && p.getTimingLocationId().equals(tlID)).map(c -> c.getTimestamp()).collect(Collectors.toList());
            startTimes.sort((p1, p2) -> p1.compareTo(p2));
            if (!startTimes.isEmpty()) startTimesByLocation.put(tlID, FXCollections.observableArrayList(startTimes));
        });
        
        // Debug output
        startTimesByLocation.keySet().forEach(k -> {
            startTimesByLocation.get(k).forEach(d -> {
                logger.debug("Found Start: TL#" + k + " -> " + DurationFormatter.durationToString(d));
            });
        });
        
        if (startTimesByLocation.isEmpty()) {
            // No start times, complain and bail
            Alert alert = new Alert(AlertType.ERROR);
            alert.setTitle("No Start Times...");
            alert.setHeaderText("No Start Times Found");
            alert.setContentText("No Start times (typically a \"0\" chip) were\nfound at any of the start timing locations!\nTo manually enter start times, update\\nthe race or wave start value on the 'Event Details' tab.");

            alert.showAndWait();
            return;
        }
        
        // for each race/wave, see if we have any start times
        Map<Integer,List<Wave>> wavesByLocation = new HashMap();
        RaceDAO.getInstance().listRaces().forEach(race -> {
            if (race.getSplits() == null || race.getSplits().isEmpty()) {
                logger.debug(" RACE HAS NO SPLITS!!! " + race.getRaceName());
                return;
            }
            Integer tlID = race.getSplits().get(0).getTimingLocationID();
            if (wavesByLocation.containsKey(tlID)) wavesByLocation.get(tlID).addAll(race.getWaves());
            else wavesByLocation.put(tlID, new ArrayList(race.getWaves()));
             
        });
        
        

        // Create the base dialog
        Dialog<List<WaveStartTime>> dialog = new Dialog();
        dialog.resizableProperty().set(true);
        dialog.getDialogPane().setMaxHeight(Screen.getPrimary().getVisualBounds().getHeight()-150);
        dialog.setTitle("Lookup Start Times");
        dialog.setHeaderText("Lookup Start Times");
        ButtonType okButtonType = new ButtonType("Save", ButtonData.OK_DONE);
        dialog.getDialogPane().getButtonTypes().addAll(okButtonType, ButtonType.CANCEL);
        
        // Create a scrollPane to put the tables and such in
        ScrollPane scrollPane = new ScrollPane();
        scrollPane.setStyle("-fx-font-size: 16px;"); // Make the scroll bar a bit larger
        VBox scrollVBox = new VBox();
        scrollVBox.setStyle("-fx-font-size: 12px;"); // Make everything normal again
        scrollVBox.fillWidthProperty().set(true);
        scrollVBox.maxHeight(Double.MAX_VALUE);
      
        scrollPane.setContent(scrollVBox);
        scrollPane.fitToWidthProperty().set(true);
        scrollPane.fitToHeightProperty().set(true);
        // For each start location, create a table and a list of possible start times
        
        List<WaveStartTime> waveStartMasterList = new ArrayList();
        
        wavesByLocation.keySet().forEach(tlID -> {
                VBox locationVBox = new VBox();
                locationVBox.fillWidthProperty().set(true);
                locationVBox.maxHeight(Double.MAX_VALUE);
                VBox.setVgrow(locationVBox, Priority.SOMETIMES);
                scrollVBox.getChildren().add(locationVBox);
                if (wavesByLocation.keySet().size()>1) {
                    Label locLabel = new Label(timingDAO.getTimingLocationByID(tlID).getLocationName() + " Timing Location");
                    locLabel.setFont(new Font(16));
                    locationVBox.getChildren().add(locLabel);
                }
                if(!startTimesByLocation.containsKey(tlID)) { // no start times
                    Label noStarts = new Label("No start times found at this location");
                    Label noStarts2 = new Label("To manually enter start times, update\nthe race or wave start value on the 'Event Details' tab.");
                    locationVBox.getChildren().add(noStarts);
                    locationVBox.getChildren().add(noStarts2);
                } else {
                    TableView<WaveStartTime> waveTable = new TableView();
                    VBox.setVgrow(waveTable, Priority.SOMETIMES);
                    waveTable.setFixedCellSize(30);
                    int size = wavesByLocation.get(tlID).size();
                    if (size >= 6) {
                        waveTable.prefHeightProperty().setValue(waveTable.getFixedCellSize()*5 + waveTable.getFixedCellSize()/2);
                        waveTable.minHeightProperty().setValue(waveTable.getFixedCellSize()*5 + waveTable.getFixedCellSize()/2);
                        //waveTable.maxHeightProperty().setValue(waveTable.getFixedCellSize()*5 + waveTable.getFixedCellSize()/2);
                        waveTable.maxHeight(Double.MAX_VALUE);
                    } else {
                        waveTable.prefHeightProperty().setValue(1+(size + 1 )* waveTable.getFixedCellSize());
                        waveTable.minHeightProperty().setValue(1+(size + 1 )* waveTable.getFixedCellSize());
                        //waveTable.maxHeightProperty().setValue(1+(size + 1 )* waveTable.getFixedCellSize());
                        waveTable.maxHeight(Double.MAX_VALUE);
                    }
                    
                    waveTable.maxWidthProperty().setValue(Double.MAX_VALUE);
                    waveTable.setEditable(true);
                    waveTable.columnResizePolicyProperty().setValue(CONSTRAINED_RESIZE_POLICY);
                    
                    TableColumn<WaveStartTime,String> waveColumn = new TableColumn("Race/Wave");
                    waveColumn.setCellValueFactory(c -> c.getValue().wName);
                    waveColumn.setEditable(false);
                    
                    TableColumn<WaveStartTime,String> oldTimeColumn = new TableColumn("Current");
                    oldTimeColumn.setPrefWidth(110);
                    oldTimeColumn.setMinWidth(oldTimeColumn.getPrefWidth());
                    oldTimeColumn.setMaxWidth(oldTimeColumn.getPrefWidth());
                    oldTimeColumn.setCellValueFactory(c -> c.getValue().wStart);
                    oldTimeColumn.setEditable(false);
                    
                    TableColumn<WaveStartTime,Duration> newTimeColumn = new TableColumn("New");
                    newTimeColumn.setPrefWidth(110);
                    newTimeColumn.setMinWidth(newTimeColumn.getPrefWidth());
                    newTimeColumn.setMaxWidth(newTimeColumn.getPrefWidth());
                    newTimeColumn.setCellValueFactory(c -> c.getValue().newStartTime);
                    newTimeColumn.setCellFactory(ComboBoxTableCell.forTableColumn(new DurationStringConverter(), startTimesByLocation.get(tlID)));
                    newTimeColumn.setOnEditCommit((TableColumn.CellEditEvent<WaveStartTime, Duration> t) -> {
                        WaveStartTime s = t.getTableView().getItems().get(t.getTablePosition().getRow());
                        s.setNewDuration(t.getNewValue());
                    });

                    TableColumn<WaveStartTime,Boolean> updateColumn = new TableColumn("Update");
                    updateColumn.setCellValueFactory(c -> c.getValue().update);
                    updateColumn.setCellFactory(tc -> new CheckBoxTableCell<>());
                    updateColumn.setEditable(true);
                    waveTable.getColumns().addAll(waveColumn,oldTimeColumn,newTimeColumn,updateColumn);

                    // create a list of waves for this location
                    ObservableList<WaveStartTime> waveStarts = FXCollections.observableArrayList();
                    wavesByLocation.get(tlID).forEach(w -> {
                        waveStarts.add(new WaveStartTime(w));
                    });
                    waveStartMasterList.addAll(waveStarts);
                    
                    // match up the waves with the best possible start time
                    // we start looking 1 minute before the scheduled start and go from there
                    // and match to the closes to the scheduled start 
                    waveStarts.sort((w1, w2) -> w1.orgStartTime.compareTo(w2.orgStartTime));
                    
                    int maxWaveIndex = waveStarts.size();
                    List<Duration> startTimes = startTimesByLocation.get(tlID); // already sorted
                    int startIndex = 0;
                    int maxStartIndex = startTimes.size();
                    
                    // Loop through the waves
                    for (int waveIndex = 0; waveIndex < maxWaveIndex; waveIndex++){
                        WaveStartTime w = waveStarts.get(waveIndex);
                        Duration d = w.orgStartTime;
                        Long millis = null; 
                        
                        logger.debug("Looking for a start trigger for " + w.wName.get() + " at " + DurationFormatter.durationToString(w.orgStartTime,3));
                        // loop through the unprocessed start times
                        for (int i = startIndex; i < maxStartIndex; i++) {
                            
                            // If it is exactly equal to the existing, stop
                            if (startTimes.get(i).equals(w.orgStartTime)) {
                                w.newStartTime.set(startTimes.get(i));
                                w.update.set(false); // unset the update flag
                                startIndex = i+1;
                                logger.debug("  Exact Match at " + DurationFormatter.durationToString(w.newStartTime.get(),3));
                                break;
                            }
                            // otherwise, if we have not assigned a time yet 
                            // or the new time is closer than the old one... 
                            if (millis == null || startTimes.get(i).minus(d).abs().toMillis() < millis) { 
                                millis = startTimes.get(i).minus(d).abs().toMillis();
                                logger.debug("  Possible Match of " + DurationFormatter.durationToString(startTimes.get(i),3) + " within " + millis + " millis away");
                                // if the new time is closer to the next wave bail
                                if (waveIndex+1 < maxWaveIndex && 
                                        millis > waveStarts.get(waveIndex+1).orgStartTime.minus(startTimes.get(i)).abs().toMillis()) {
                                    logger.debug("  But it is closer to the next time of " + DurationFormatter.durationToString(waveStarts.get(waveIndex+1).orgStartTime,3));
                                    logger.debug("  which is only " + waveStarts.get(waveIndex+1).orgStartTime.minus(startTimes.get(i)).abs().toMillis() + " millis away");
                                    break;
                                } 
                                w.newStartTime.set(startTimes.get(i));
                                w.update.set(true);
                                startIndex = i+1;
                            } else {
                                logger.debug("  Possible Match of " + DurationFormatter.durationToString(startTimes.get(i),3) + " is " + startTimes.get(i).minus(d).abs().toMillis() + " millis away");
                                break;
                            }
                        }
                    }
                    


                    waveTable.setItems(waveStarts);
                    locationVBox.getChildren().add(waveTable);
                }
            
        });
        
        Label manualAddLabel = new Label("To manuall adjust the start times,\ngo to the Event Details tab.");
         manualAddLabel.setFont(new Font(16));
        scrollVBox.getChildren().add(manualAddLabel);
    
        


        
        dialog.getDialogPane().setContent(scrollPane);
        
        
        dialog.setResultConverter(dialogButton -> {
            if (dialogButton == okButtonType) {
                return waveStartMasterList;
            }
            return null;
        });
        
        Platform.runLater(() -> { dialog.setY((Screen.getPrimary().getVisualBounds().getHeight()-dialog.getHeight())/2); });
        Optional<List<WaveStartTime>> result = dialog.showAndWait();
        if (result.isPresent()) {
            // Walk the map
            List<WaveStartTime> results = result.get();
            results.forEach(k -> {
                if (k.update.get()) {
                    k.w.setWaveStart((LocalTime.MIDNIGHT.plus(k.newStartTime.get())).format(DateTimeFormatter.ISO_LOCAL_TIME));
                    ResultsDAO.getInstance().reprocessWaveResults(k.w);
                    RaceDAO.getInstance().updateWave(k.w);
                }
            });
        }
    }
    
    // What the name column shows for a time that does not belong to a participant
    private static String unregisteredName(CookedTimeData c) {
        String bib = c.getBib();
        if (bib == null) return "";
        if (bib.startsWith("Unmapped")) return "Unknown Chip";
        if ("0".equals(c.getRawChipID())) return "START TRIGGER";
        return "Unregistered bib: " + bib;
    }
    
    // bib -> full name for every participant. The participant map in the 
    // ParticipantDAO is a plain HashMap and not safe to read from the 
    // background sort/filter thread, so take a copy here. 
    private Map<String,String> participantNameSnapshot(boolean stripAccents) {
        Map<String,String> names = new HashMap<>();
        participantDAO.listParticipants().forEach(p -> {
            String bib = p.getBib();
            if (bib == null || bib.isEmpty()) return;
            String name = p.fullNameProperty().getValueSafe();
            names.put(bib, stripAccents ? StringUtils.stripAccents(name) : name);
        });
        return names;
    }
    
    private void updateFilterPredicate(){
        logger.trace("updateFilterPredicate called...");
        
        final String filterText = searchTextBox.textProperty().getValueSafe();
        final Set<Integer> locationIDs = new HashSet<>();
        searchScopeCheckComboBox.getCheckModel().getCheckedItems().forEach(tl -> {
            if (tl != null) locationIDs.add(tl.getID());
        });
        
        // If both are empty, display everything
        if (filterText.isEmpty() && locationIDs.isEmpty()) {
            timeView.setFilter(null);
            return;
        }
        
        // Everything the predicate needs is captured here, it runs on a 
        // background thread. 
        final String needle = StringUtils.stripAccents(filterText);
        final Map<String,String> names = filterText.isEmpty() ? Map.of() : participantNameSnapshot(true);
        
        timeView.setFilter(cookedRead -> {
            if (!locationIDs.isEmpty() && !locationIDs.contains(cookedRead.getTimingLocationId())) return false;
            if (needle.isEmpty()) return true;
            
            // Match the name, chip, or bib
            if (StringUtils.containsIgnoreCase(cookedRead.getBib(), needle)) return true;
            if (StringUtils.containsIgnoreCase(cookedRead.getRawChipID(), needle)) return true;
            String name = names.get(cookedRead.getBib());
            if (name == null) name = unregisteredName(cookedRead);
            return StringUtils.containsIgnoreCase(name, needle);
        });
    }
    
    // Turn the table's sort order into sort keys for the background list.
    // Like the filter, the keys can only use snapshots. 
    private void updateSort() {
        List<BackgroundSortedFilteredList.SortKey<CookedTimeData>> keys = new ArrayList<>();
        
        Map<String,String> names = null;
        Map<Integer,String> locationNames = new HashMap<>();
        Map<Integer,String> inputNames = new HashMap<>();
        timingLocationList.forEach(tl -> {
            locationNames.put(tl.getID(), tl.getLocationName());
            tl.getInputs().forEach(tli -> inputNames.put(tli.getID(), tli.getLocationName()));
        });
        
        for (TableColumn<CookedTimeData,?> col: timeTableView.getSortOrder()) {
            boolean desc = col.getSortType() == TableColumn.SortType.DESCENDING;
            if (col == bibColumn) {
                keys.add(BackgroundSortedFilteredList.SortKey.of(CookedTimeData::getBib, new AlphanumericComparator<String>(), desc));
            } else if (col == chipColumn) {
                keys.add(BackgroundSortedFilteredList.SortKey.of(CookedTimeData::getRawChipID, new AlphanumericComparator<String>(), desc));
            } else if (col == timeColumn) {
                keys.add(BackgroundSortedFilteredList.SortKey.natural(CookedTimeData::getTimestamp, desc));
            } else if (col == nameColumn) {
                if (names == null) names = participantNameSnapshot(false);
                final Map<String,String> n = names;
                keys.add(BackgroundSortedFilteredList.SortKey.natural(c -> {
                    String name = n.get(c.getBib());
                    return name != null ? name : unregisteredName(c);
                }, desc));
            } else if (col == locationColumn) {
                keys.add(BackgroundSortedFilteredList.SortKey.natural(c -> locationNames.getOrDefault(c.getTimingLocationId(), "Unknown"), desc));
            } else if (col == inputColumn) {
                keys.add(BackgroundSortedFilteredList.SortKey.natural(c -> inputNames.getOrDefault(c.getTimingLocationInputId(), "Unknown"), desc));
            } else if (col == backupColumn) {
                keys.add(BackgroundSortedFilteredList.SortKey.natural(CookedTimeData::getBackupTime, desc));
            } else if (col == ignoreColumn) {
                keys.add(BackgroundSortedFilteredList.SortKey.natural(CookedTimeData::getIgnoreTime, desc));
            }
        }
        timeView.setSortKeys(keys);
    }
    
    // Selection and scroll position to put back after the time view changes
    private CookedTimeData selectedTimeBeforeUpdate;
    private CookedTimeData topTimeBeforeUpdate;
    
    private void beforeTimeViewUpdate(BackgroundSortedFilteredList.Update kind) {
        selectedTimeBeforeUpdate = timeTableView.getSelectionModel().getSelectedItem();
        topTimeBeforeUpdate = null;
        if (kind == BackgroundSortedFilteredList.Update.ROWS_ADDED) return;
        
        // A whole new list (a resort, a new filter, or a reprocess)
        if (timeTableView.lookup(".virtual-flow") instanceof VirtualFlow<?> flow) {
            IndexedCell<?> first = flow.getFirstVisibleCell();
            if (first != null && first.getIndex() >= 0 && first.getIndex() < timeView.size()) {
                topTimeBeforeUpdate = timeView.get(first.getIndex());
            }
        }
        // The TableView is slow to work out where a selected row went in a 
        // very large replace, and we put it back ourselves anyway
        if (selectedTimeBeforeUpdate != null) timeTableView.getSelectionModel().clearSelection();
    }
    
    private void afterTimeViewUpdate(BackgroundSortedFilteredList.Update kind) {
        int selected = timeView.indexOfIdentity(selectedTimeBeforeUpdate);
        if (selected >= 0 && timeTableView.getSelectionModel().getSelectedIndex() != selected) {
            timeTableView.getSelectionModel().clearAndSelect(selected);
        } else if (selected < 0 && selectedTimeBeforeUpdate != null) {
            timeTableView.getSelectionModel().clearSelection();
        }
        
        if (kind == BackgroundSortedFilteredList.Update.VIEW_CHANGED) {
            // A new view: show the selected time, or start at the top
            timeTableView.scrollTo(Math.max(selected, 0));
        } else if (kind == BackgroundSortedFilteredList.Update.SOURCE_REPLACED) {
            // Same view, new data: try to stay where we were
            int top = timeView.indexOfIdentity(topTimeBeforeUpdate);
            if (top >= 0) timeTableView.scrollTo(top);
        }
        selectedTimeBeforeUpdate = null;
        topTimeBeforeUpdate = null;
    }

    private class IgnoreFlagTableCell extends TableCell<CookedTimeData, Boolean> {
        private final CheckBox checkBox = new CheckBox();
        
        IgnoreFlagTableCell() {
            setAlignment(Pos.CENTER);
            setContentDisplay(ContentDisplay.GRAPHIC_ONLY);
            checkBox.setOnAction(e -> {
                CookedTimeData c = getTableRow() == null ? null : getTableRow().getItem();
                if (c == null) return;
                c.setIgnoreTime(checkBox.isSelected());
                logger.debug("CookedTimeData " + c.getBib() + " changed value to " + c.getIgnoreTime());
                timingDAO.saveCookedTime(c);
                if (timeTableView.getSortOrder().contains(ignoreColumn)) updateSort();
            });
        }
        
        @Override
        protected void updateItem(Boolean ignore, boolean empty) {
            super.updateItem(ignore, empty);
            if (empty || ignore == null) {
                setGraphic(null);
            } else {
                checkBox.setSelected(ignore);
                setGraphic(checkBox);
            }
        }
    }

        private class BackupFlagTableCell extends TableCell<CookedTimeData, Boolean> {
        @Override
        protected void updateItem(Boolean d, boolean empty) {
            super.updateItem(d, empty);
            if (d == null || empty) {
                setText(null);
            } else {
                // Format duration.
                CookedTimeData t = (CookedTimeData)getTableRow().getItem();
                if (t == null) {
                    setText("");
                    return;
                }
                
                if (! t.getBackupTime()) setText("-");
                else setText("X");
            }
        }

    };
        
    private static class WaveStartTime {
        public Wave w;
        public StringProperty wName = new SimpleStringProperty();
        public StringProperty wStart = new SimpleStringProperty();
        public ObjectProperty<Duration> newStartTime = new SimpleObjectProperty(Duration.ZERO);
        public BooleanProperty update = new SimpleBooleanProperty(false);
        public Duration orgStartTime;
                
        public WaveStartTime() {
        }
        public WaveStartTime(Wave w){
            this.setWave(w);
        }
        
        public void setWave(Wave wave){
            w=wave;
            wName.setValue(WaveStringConverter.getString(w));
            wStart.bind(w.waveStartStringProperty());
            newStartTime.setValue(Duration.between(LocalTime.MIDNIGHT, w.waveStartProperty()));
            orgStartTime=newStartTime.get();
        }
        
        public void setNewDuration(Duration d){
            newStartTime.set(d);
            if (orgStartTime.equals(d)) update.set(false);
            else update.set(true);
        }
        
    }
    
}
