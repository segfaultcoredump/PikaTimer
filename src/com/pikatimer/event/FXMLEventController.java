/*
 *  Copyright 2014 John Garner. All rights reserved. 

 */
package com.pikatimer.event;



import com.pikatimer.race.FXMLRaceDetailsController;
import com.pikatimer.race.Race;
import com.pikatimer.race.RaceDAO;
import com.pikatimer.timing.TimingLocation;
import com.pikatimer.timing.TimingDAO;
import com.pikatimer.util.Unit;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.beans.value.ObservableValue;
import javafx.collections.ListChangeListener;
import javafx.collections.ListChangeListener.Change;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.DatePicker;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.cell.TextFieldListCell;
import javafx.scene.layout.Pane;
import javafx.scene.layout.VBox;
import javafx.util.StringConverter;

/**
 * FXML Controller class
 *
 * @author jcgarner
 */
public class FXMLEventController  {
    
    private final Event event = Event.getInstance();
    private EventDAO eDAO; 
    private FXMLLoader raceDetailsLoader ;
    @FXML private TextField eventTitle;
    @FXML private DatePicker eventDate;
    @FXML private CheckBox multipleRacesCheckBox;
    @FXML private VBox racesVBox; 
    @FXML private TableView<Race> raceTableView; 
    @FXML private Button raceRemoveAllButton;
    @FXML private Button raceAddButton;
    @FXML private Button raceRemoveButton;  
    private ObservableList<Race> raceList; 
    @FXML private CheckBox multipleTimingCheckBox;
    @FXML private VBox timingVBox;
    @FXML private ListView<TimingLocation> timingLocListView;
    @FXML private Button timingLocRemoveAllButton;
    @FXML private Button timingLocAddButton;
    @FXML private Button timingLocRemoveButton;  
    private ObservableList<TimingLocation> timingLocationList;
    private TimingDAO timingLocationDAO; 
    private FXMLRaceDetailsController raceDetailsController;
    private RaceDAO raceDAO;
    
    //@FXML private Pane raceDetailsPane;
    @FXML private VBox raceDetailsVBox;
    
    /**
     * Initializes the controller class.
     */
   @FXML
    protected void initialize() {
        // TODO
        System.out.println("FXMLpikaController initialize called...");
        
        
        
        eventTitle.setText(event.getEventName());
        System.out.println("FXMLpikaController initialize set title");
        
        
        
        //Watch for text changes... Because setOnInputMethodTextChanged does not work :-( 
        eventTitle.focusedProperty().addListener((ObservableValue<? extends Boolean> arg0, Boolean oldPropertyValue, Boolean newPropertyValue) -> {
            if (!newPropertyValue) {
                System.out.println("Textfield out focus");
                if ( ! eventTitle.getText().equals(event.getEventName()) ) {
                    setEventTitle();
                }
            }
        });
        
        // Use this if you whant keystroke by keystroke monitoring.... 
        //        eventTitle.textProperty().addListener((observable, oldValue, newValue) -> {
        //            //System.out.println("TextField Text Changed (newValue: " + newValue + ")");
        //            event.setEventName(newValue);
        //            updateEvent();
        //        });
        
        eventDate.setValue(event.getLocalEventDate());
        
        
        /** Set the actions after we seed the value, otherwise the eventDate
         * action will fire and call an update... ugh... 
         * 
        */
        eventDate.setOnAction(this::setEventDate);
        //eventDate.onInputMethodTextChangedProperty()
        //eventDate.onInputMethodTextChanged(this::setEventDate);
        System.out.println("FXMLpikaController initialize set date");
        
        //event.multipleRacesProperty().bind(singleRaceCheckBox.selectedProperty());
        
        //Setup the races VBox 
        // Bind the multiple Races CheckBox to the races table to automatically 
        // enable / disable it
        timingVBox.managedProperty().bind(multipleTimingCheckBox.selectedProperty());
        timingVBox.visibleProperty().bind(multipleTimingCheckBox.selectedProperty());
        // if we have more than one race then let's set the multipleRacesCheckBox to true.
        multipleTimingCheckBox.setSelected(false);
        
        timingLocationDAO=TimingDAO.getInstance();
        timingLocationList=timingLocationDAO.listTimingLocations(); 
        if (timingLocationList.isEmpty()) {
            timingLocationDAO.createDefaultTimingLocations();
        }
        
        timingLocListView.setItems(timingLocationList);
        
        if (timingLocationList.size() > 2){
            multipleTimingCheckBox.setSelected(true);
        }
        
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
                    System.out.println("Timing StringConverter toString null object detected.");
                    return "";
                }
            }
        }        
        ));		

        timingLocListView.setOnEditCommit((ListView.EditEvent<TimingLocation> t) -> {
            System.out.println("setOnEditCommit " + t.getIndex());
            if(t.getIndex() < t.getSource().getItems().size()) {
                TimingLocation tl = t.getSource().getItems().get(t.getIndex()); 
                if (t.getNewValue().toString().isEmpty()) {
                    //timingLocationDAO.removeTimingLocation(tl);
                    //tl.setLocationName("New Timing Location");
                    timingLocationDAO.removeTimingLocation(tl);
                } else {
                    tl.setLocationName(t.getNewValue().toString());
                    timingLocationDAO.updateTimingLocation(tl);
                }
            } else {
                System.out.println("Timing setOnEditCommit event out of index: " + t.getIndex());
            }
            timingLocAddButton.requestFocus();
            timingLocAddButton.setDefaultButton(true);
        });

        timingLocListView.setOnEditCancel((ListView.EditEvent<TimingLocation> t) ->{
            System.out.println("setOnEditCancel " + t.getIndex());
            if (t.getIndex() >= 0 && t.getIndex() < t.getSource().getItems().size()) {
                TimingLocation tl = t.getSource().getItems().get(t.getIndex());
                if (tl.getLocationName().isEmpty()) {
                    //tl.setLocationName("New Timing Location");
                    timingLocationDAO.removeTimingLocation(tl);
                }
            } else {
                System.out.println("Timing setOnEditCancel event out of index: " + t.getIndex());
            }
            timingLocAddButton.requestFocus();
            timingLocAddButton.setDefaultButton(true);
        });
        
        timingLocRemoveButton.disableProperty().bind(timingLocListView.getSelectionModel().selectedItemProperty().isNull());
        //Setup the races VBox 
        // Bind the multiple Races CheckBox to the races table to automatically 
        // enable / disable it
        racesVBox.managedProperty().bind(multipleRacesCheckBox.selectedProperty());
        racesVBox.visibleProperty().bind(multipleRacesCheckBox.selectedProperty());
        
        
        // Populate the underlying table with any races.
        // raceDAO.getRaces(); 
        
        // if we have more than one race then let's set the multipleRacesCheckBox to true.
        multipleRacesCheckBox.setSelected(false);
        
       
        raceDAO=RaceDAO.getInstance();
        raceList=raceDAO.listRaces(); 
        Race selectedRace;
        if (raceList.isEmpty()) {
            selectedRace = new Race(); 
            selectedRace.setRaceName(event.getEventName());
            selectedRace.setRaceDistance(new BigDecimal("5.0")); 
            selectedRace.setRaceDistanceUnits(Unit.KILOMETERS);
            raceDAO.addRace(selectedRace);
        } else {
            selectedRace = raceList.get(0); 
        }
        
        raceTableView.setItems(raceList);
        
        raceRemoveButton.setDisable(true);

        if (raceList.size() > 1){
            multipleRacesCheckBox.setSelected(true);
            multipleRacesCheckBox.setDisable(true);
            raceRemoveButton.setDisable(false);

        }
        
        
        // load up the raceDetailsPane
        // Save the FXMLLoader so that we can send it notes when things change in the races box
        raceDetailsVBox.getChildren().clear();
            try {
                raceDetailsLoader = new FXMLLoader(getClass().getResource("/com/pikatimer/race/FXMLRaceDetails.fxml"));
                raceDetailsVBox.getChildren().add(raceDetailsLoader.load());
            } catch (IOException ex) {
                Logger.getLogger(FXMLEventController.class.getName()).log(Level.SEVERE, null, ex);
                ex.printStackTrace();
            }
        
            raceDetailsController =(FXMLRaceDetailsController)raceDetailsLoader.getController(); 
            raceDetailsController.selectRace(selectedRace);
         // bind the selected race to the
         //FXMLRaceDetailsController raceDetailsController = raceDetailsLoader.<FXMLRaceDetailsController>getController(); 
         //raceDetailsController.selectRace(r);
            
         //if there are no races selected in the race table then disable the entire right hand side
         raceDetailsVBox.disableProperty().bind(raceTableView.getSelectionModel().selectedItemProperty().isNull());
         raceTableView.getSelectionModel().getSelectedItems().addListener((ListChangeListener.Cha‌​nge<? extends Race> c) -> { 
             raceTableView.getSelectionModel().getSelectedItems().forEach(System.out::println); 
             ObservableList<Race> selectedRaces = raceTableView.getSelectionModel().getSelectedItems();
             if ( selectedRaces.size() == 0 ) {
                System.out.println("Nothing Selected");
                raceDetailsController.selectRace(null);
             } else {
                System.out.println(selectedRaces.get(0).getRaceName());
                raceDetailsController.selectRace(selectedRaces.get(0));
             }
         });
         
         raceTableView.getSelectionModel().clearAndSelect(0);
//         raceTableView.getSelectionModel().getSelectedIndices().addListener((Change<? extends Integer> change) -> {
////                if (change.getList().size() == 1) {
////                    multipleRacesCheckBox.setDisable(false);
////                    //raceDetailsPane.setDisable(true);
////                } else {
////                    //raceDetailsPane.setDisable(false); 
////                    multipleRacesCheckBox.setDisable(true);
////                }
//             System.out.println("raceTableView selection changed... ");
//             raceDetailsController.selectRace(raceTableView.getSelectionModel().getSelectedItem());
//
//            });
         
         
         
         
        System.out.println("FXMLpikaController initialized!");
        
    }
    
    /**
     * Initializes the controller class.
     * @param fxevent
     */
    @FXML
    protected void setEventTitle(ActionEvent fxevent) {
//        event.setEventName(eventTitle.getText());
//        updateEvent();
        setEventTitle();
    }
    
    protected void setEventTitle() {
        event.setEventName(eventTitle.getText());
        updateEvent();
        
        // If there is only one race, also update the race name
        if(raceList.size()== 1){
            Race selectedRace = raceList.get(0); 
            selectedRace.setRaceName(eventTitle.getText());
            raceDAO.updateRace(selectedRace);
        }
    }
    
    @FXML
    protected void setEventDate(ActionEvent fxevent) {
        
        event.setEventDate(eventDate.getValue());
        updateEvent();
           
    }
    
//    @FXML
//    protected void toggleSingleRaceCheckBox(ActionEvent fxevent) {
//        // are we enabled or disabled?
//        if ( singleRaceCheckBox.isSelected() ) {
//            System.out.println("Only one race...");
//            
//            // load the single race fxml into the singleRacePane; 
//            
//            event.getMainTabPane().getTabs().removeIf(p -> p.getText().equals("Races"));
//            singleRacePane.getChildren().clear();
//            try {
//                singleRacePane.getChildren().add(FXMLLoader.load(getClass().getResource("/com/pikatimer/race/FXMLSingleRace.fxml")));
//            } catch (IOException ex) {
//                Logger.getLogger(FXMLEventController.class.getName()).log(Level.SEVERE, null, ex);
//                ex.printStackTrace();
//            }
//        } else {
//            // More than one race. Show a placeholder text and open the Races Tab
//            System.out.println("More than one race...");
//            singleRacePane.getChildren().clear();
//            singleRacePane.getChildren().add(new Label("Multiple Races"));
//            Tab raceTab = new Tab("Races");
//            try {
//                raceTab.setContent(FXMLLoader.load(getClass().getResource("/com/pikatimer/race/FXMLMultipleRaces.fxml")));
//            } catch (IOException ex) {
//                Logger.getLogger(FXMLEventController.class.getName()).log(Level.SEVERE, null, ex);
//                ex.printStackTrace();
//            }
//            event.getMainTabPane().getTabs().add(1, raceTab);
//        }
//        
//    }
    
    private void updateEvent() {
        if (eDAO == null) {
            eDAO = new EventDAO(); 
        }
        eDAO.updateEvent();
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
            timingLocationDAO.createDefaultTimingLocations();
        } else {
            // ... user chose CANCEL or closed the dialog
        }
        
        timingLocAddButton.requestFocus();
        timingLocAddButton.setDefaultButton(true);
    }
        
    public void addTimingLocation(ActionEvent fxevent){
        // prompt 
        TimingLocation t = new TimingLocation();
        t.setLocationName("New Timing Location");
        
        timingLocationDAO.addTimingLocation(t);
        System.out.println("Setting the timingLocListView.edit to " + timingLocationList.size() + " " + timingLocationList.indexOf(t));
        timingLocListView.getSelectionModel().select(timingLocationList.indexOf(t));
        timingLocListView.edit(timingLocationList.indexOf(t));
        
        //Because we call the timingLocListView.edit, we don't want to pull back focus
        //timingLocAddButton.requestFocus();

    }
    public void removeTimingLocation(ActionEvent fxevent){
        //TODO: If the location is referenced by a split, 
        //prompt to reassign the split to a new location or cancel the edit. 
        
        timingLocationDAO.removeTimingLocation(timingLocListView.getSelectionModel().getSelectedItem());
        timingLocAddButton.requestFocus();
        timingLocAddButton.setDefaultButton(true);
    }
    
    public void resetRaces(ActionEvent fxevent){
        // prompt 
        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Confirm Resetting All Races");
        alert.setContentText("This action cannot be undone.");
        alert.setHeaderText("This will delete all configured races!");
        //Label alertContent = new Label("This will reset the timing locations to default values.\nAll splits will be reassigned to one of the default locations.");
        //alertContent.setWrapText(true); 
        //alert.getDialogPane().setContent(alertContent);
        
        Optional<ButtonType> result = alert.showAndWait();
        if (result.get() == ButtonType.OK){
            raceDAO.clearAll();
        } else {
            // ... user chose CANCEL or closed the dialog
        }
        
        raceAddButton.requestFocus();
        raceAddButton.setDefaultButton(true);
    }
        
    public void addRace(ActionEvent fxevent){
        // prompt 
        Race r = new Race();
        r.setRaceName("New Race");
        r.setRaceDistance(new BigDecimal("5.0")); 
        r.setRaceDistanceUnits(Unit.KILOMETERS);
        raceDAO.addRace(r);
        System.out.println("Adding a new race. New Size =" + raceList.size() + " New Race Index=" + raceList.indexOf(r));
        raceTableView.getSelectionModel().select(raceList.indexOf(r));
        //timingLocListView.edit(timingLocationList.indexOf(t));
        if (raceList.size() > 1) { 
            multipleRacesCheckBox.setDisable(true);
            raceRemoveButton.setDisable(false); 
        } else {
            multipleRacesCheckBox.setDisable(false);
            raceRemoveButton.setDisable(true);
        }
        raceAddButton.setDefaultButton(false);
        //Because we call the timingLocListView.edit, we don't want to pull back focus
        //timingLocAddButton.requestFocus();

    }
    public void removeRace(ActionEvent fxevent){
        //TODO: If the location is referenced by a split, 
        //prompt to reassign the split to a new location or cancel the edit. 
        
        raceDAO.removeRace(raceTableView.getSelectionModel().getSelectedItem());
        raceTableView.getSelectionModel().select(raceList.indexOf(0));
        raceAddButton.requestFocus();
        raceAddButton.setDefaultButton(false);
        
        if (raceList.size() > 1) { 
            multipleRacesCheckBox.setDisable(true);
            raceRemoveButton.setDisable(false); 
        } else {
            multipleRacesCheckBox.setDisable(false);
            raceRemoveButton.setDisable(true);
        }
        
       
    }
}
