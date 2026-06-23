/*
 * Copyright (C) 2025 John Garner <segfaultcoredump@gmail.com>
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
package com.pikatimer.participant.rsu;

import com.pikatimer.PikaPreferences;
import com.pikatimer.event.Event;
import com.pikatimer.participant.Participant;
import com.pikatimer.participant.ParticipantDAO;
import com.pikatimer.race.Race;
import com.pikatimer.race.RaceDAO;
import com.pikatimer.race.Wave;
import com.pikatimer.util.HibernateUtil;
import com.pikatimer.util.StringCapitalizationNormalizer;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import javafx.application.Platform;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import javafx.geometry.Pos;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonBar.ButtonData;
import javafx.scene.control.ButtonType;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.PasswordField;
import javafx.scene.control.ProgressBar;
import javafx.scene.control.SelectionMode;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import org.controlsfx.control.ToggleSwitch;
import org.controlsfx.dialog.Wizard;
import org.controlsfx.dialog.WizardPane;
import org.hibernate.Session;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author John Garner <segfaultcoredump@gmail.com>
 */

//////
//
// WARNING: this is a hack and a half from a thread and process flow standpoint.
// It should probably be refactored at some point in the future. 
//
//
//  TODO:
//     * if username/key starts with "TEST:" then use the test.runsignup.com
//       endpoint and adjust.
//     * Error checking and recovery needs to be implemented. 
//
/////////

public class RunSignUpDAO {

    private static final Logger logger = LoggerFactory.getLogger(RunSignUpDAO.class);
    private RSUConfig rsuConfig;

    //private final Event event = Event.getInstance();

    private final BooleanProperty isSetup = new SimpleBooleanProperty(false); 
    
    private final ParticipantDAO partDAO = ParticipantDAO.getInstance();
    private final RaceDAO raceDAO = RaceDAO.getInstance();
    
    Boolean syncInProgress = false;


    private static class SingletonHolder {

        private static final RunSignUpDAO INSTANCE = new RunSignUpDAO();
    }

    public static RunSignUpDAO getInstance() {

        return SingletonHolder.INSTANCE;
    }
    
    

    public BooleanProperty isSetup() {
        if (rsuConfig == null) {
            getRSUConfig();
        }

        return isSetup;
    }
    
    public Boolean syncInProgress(){
        return syncInProgress;
    }

    private RSUConfig getRSUConfig() {

        if (rsuConfig == null) {
            final List<RSUConfig> list;

            // Let's see if we have anything in the DB... 
            Session s = HibernateUtil.getSessionFactory().getCurrentSession();
            s.beginTransaction();

            logger.debug("RunSignUpDAO:: loading existing RSU Config from db");

            try {
                list = s.createQuery("from RSUConfig").list();

                logger.debug("RunSignUpDAO::getRSUConfig found " + list.size() + " rsu configs");

                if (!list.isEmpty()) {
                    rsuConfig = list.getFirst();
                    isSetup.set(true);
                }
            } catch (Exception e) {
                logger.debug(e.getMessage());
            }
            s.getTransaction().commit();
        }

        // are we still null? 
        if (rsuConfig == null) {
            rsuConfig = new RSUConfig();
            return rsuConfig;
        }

        return rsuConfig;
    }
    
    public void syncWithRSU(ProgressBar progressBar, Label progressLabel){
        
        if (syncInProgress) return; 
         
        // We don't want to block the JavaFX thread, so we dump this into a task. 
        
        Task rsuSyncTask = new Task<Void>() {
            @Override
            protected Void call() {
                Boolean okToGo = true; 
                okToGo = syncToRSU();
                if (okToGo) syncFromRSU(progressBar,progressLabel);
            
                return null;
            }
        };

        Thread resync = new Thread(rsuSyncTask);
        
        resync.setDaemon(true);
        resync.start();
    }

    public void syncFromRSU(ProgressBar progressBar, Label progressLabel) {
        logger.debug("RunSignUpDAO::syncFromRSU() Start...");

        // Check the config
        if (!isSetup.get()) {
            logger.warn("RSUConfig not setup, not syncing!");
            return;
        }

        Task resyncTask = new Task<Void>() {
            @Override
            protected Void call() {
                logger.debug("SyncFromRSU Thread Started");
                syncInProgress = true;

                updateProgress(0, 100);

                // if we are useing a username / password, (re)generate the tmp_key and tmp_secret
                if (!rsuConfig.rsuLoginType.equals("API")) {
                    updateRSUKeys();
                }
                
                
                Map<Integer,Participant> regIDtoParticipantMap = new HashMap<>();
                Map<Integer,Participant> userIDtoParticipantMap = new HashMap<>();
                
                partDAO.listParticipants().forEach(p -> {
                    userIDtoParticipantMap.put(p.getRegUserID(), p);
                    p.getRegID2RaceIDDMap().keySet().forEach(reg -> {regIDtoParticipantMap.put(reg,p);});
                
                });
                
                // Quick count for the progress bar
                Integer eventsToProcess = 0;
                Integer eventsProcessed = 0;
                for (Integer a: rsuConfig.eventToRaceMap.keySet())
                    if (!rsuConfig.eventToRaceMap.get(a).equals(-1)) eventsToProcess+=2;
                
                // Timestamp to track when we last synced w/ RSU                
                Long lastRunTS = Instant.now().getEpochSecond();
                
                // default pageSize for RSU Requests
                Integer pageSize = 1000;
                
                // For each Event_ID that is not set to "IGNORE", download the participants
                for (Integer event: rsuConfig.eventToRaceMap.keySet()) {
                    if (!rsuConfig.eventToRaceMap.get(event).equals(-1)) { 
                        Integer regReturned = 0;
                        Integer page = 1;
                        logger.info("Getting registrations for Race with event_id {} from RSU", event);
                        
                        
                        Race race = RaceDAO.getInstance().getRaceByID(rsuConfig.eventToRaceMap.get(event));
                        Boolean multipleWaves = race.getWaves().size() > 1;
                        
                        Wave defaultWave = race.getWaves().getLast();
                        
                        updateMessage("Syncing " + race.getRaceName() + "...");
                                         
                        try {
                            do {

                                StringBuilder rsuURL = new StringBuilder();
                                rsuURL.append("https://runsignup.com/Rest/race/").append(rsuConfig.rsuRaceID);
                                rsuURL.append("/participants?format=json&event_id=").append(event);
                                rsuURL.append("&page=").append(page.toString()).append("&results_per_page=").append(pageSize);
                                rsuURL.append("&modified_after_timestamp=").append(rsuConfig.rsuLastSync);
                                rsuURL.append("&include_user_anonymous_flag=T&include_questions=T&include_registration_addons=T&supports_nb=T");
                                
                                // log it now before we tack on the key and secret
                                logger.debug("Participant request for race_id={} and event_id={}: {}", rsuConfig.rsuRaceID, event, rsuURL.toString());
                                
                                if (rsuConfig.rsuLoginType.equals("API")) {
                                    rsuURL.append("&api_key=").append(rsuConfig.rsuKey);
                                    rsuURL.append("&api_secret=").append(rsuConfig.rsuSecret);
                                } else {
                                    updateRSUKeys();
                                    rsuURL.append("&tmp_key=").append(rsuConfig.rsuTempKey);
                                    rsuURL.append("&tmp_secret=").append(rsuConfig.rsuTempSecret);
                                }
                                
                                HttpRequest request = HttpRequest.newBuilder()
                                        .uri(URI.create(rsuURL.toString()))
                                        .build();

                                HttpClient client = HttpClient.newHttpClient();

                                try {
                                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                                    if (response.statusCode() == 200) {

                                        if (response.body().startsWith("[{")) { // We have a json array....
                                            try {
                                                JSONArray rsuResponse = new JSONArray(response.body());
                                                logger.trace("RSU Raw Response: {} ",rsuResponse.toString(4));
                                                if (rsuResponse.getJSONObject(0).isNull("participants")) {
                                                    logger.debug("No participants returned.");
                                                    break;
                                                }       
                                                
                                                JSONArray results = rsuResponse.getJSONObject(0).getJSONArray("participants");
                                                logger.trace(results.toString(4));

                                                regReturned = results.length();
                                                for (int j = 0; j < results.length(); j++) {
                                                    JSONObject rsuReg = results.getJSONObject(j);
                                                                                                            
                                                    Participant p;
                                                    if (regIDtoParticipantMap.containsKey(rsuReg.optIntegerObject("registration_id"))) {
                                                        p = regIDtoParticipantMap.get(rsuReg.optIntegerObject("registration_id"));
                                                        logger.trace("RSUSync: Found existing RSU RegistrationID {} for {}", rsuReg.optIntegerObject("registration_id"), p.fullNameProperty().getValue() );
                                                    } else if (userIDtoParticipantMap.containsKey(rsuReg.getJSONObject("user").optIntegerObject("user_id"))) { 
                                                        p = userIDtoParticipantMap.get(rsuReg.getJSONObject("user").optIntegerObject("user_id"));
                                                        logger.trace("RSUSync: Found existing RSU UserID {} for {}", rsuReg.getJSONObject("user").optIntegerObject("user_id"), p.fullNameProperty().getValue() );
                                                    } else {
                                                        p = new Participant();
                                                        logger.trace("RSUSync: unable to find an existing registration or user, creating a new user....");
                                                        p.setRegSyncNeeded(false);
                                                    }
                                                    
                                                    // If the particiopant is pending a sync to RSU, skip it.
                                                    // if (p.getRegSyncNeeded()) break;
                                                    
                                                    
                                                    
                                                    CountDownLatch platformDone = new CountDownLatch(1);
                                                    Platform.runLater(() -> {    

                                                        // Basic RSU Attributes
                                                        // ("First_Name", "Middle_Name", "Last_Name"));
                                                        // ("Gender", "Age", "DOB", "Bib"));
                                                        // ("City", "State", "Country"));
                                                        // ("EMail", "Giveaway", "isAnonymous", "Team_Name"));
                                                        //
                                                        // Everything else is a question prompt text

                                                        p.setBib(rsuReg.optString("bib_num"));

                                                        JSONObject pJSON = rsuReg.getJSONObject("user");

                                                        // set the RSU UserID
                                                        p.setRegUserID(pJSON.optIntegerObject("user_id"));


                                                        p.setFirstName(pJSON.optString("first_name"));
                                                        p.setMiddleName(pJSON.optString("middle_name"));
                                                        p.setLastName(pJSON.optString("last_name"));
                                                        p.setEmail(pJSON.optString("email"));
                                                        p.setSex(pJSON.optString("gender"));
                                                        //if (pJSON.optString("is_anonymous").equalsIgnoreCase("T")) p.setIsAnon(Boolean.TRUE);
                                                        p.setAge(rsuReg.optIntegerObject("age"));
                                                        p.setBirthday(pJSON.optString("dob"));
                                                        

                                                        JSONObject aJSON = pJSON.getJSONObject("address");
                                                        p.setCity(aJSON.optString("city"));
                                                        p.setState(aJSON.optString("state"));
                                                        p.setZip(aJSON.optString("zip"));
                                                        p.setCountry(aJSON.optString("country_code"));
                                                        
                                                        
                                                        // Cleanup the name / city     
                                                        
                                                        // look for inadvertant reduplication
                                                        // e.g "Colorado SpringsColorado Springs"
                                                        // Actual reduplicated names will have an odd length (like "Walla Walla")
                                                        if (p.getCity().length() % 2 == 0) {
                                                            String c = p.getCity();
                                                            if (c.toLowerCase().substring(0, (c.length()/2)).equals(c.toLowerCase().substring(c.length()/2, c.length()))) {
                                                                c = c.substring(0, (c.length()/2));
                                                                logger.debug("Reduplicated City: {} -> {}",p.getCity(),c);
                                                                p.setCity(c);
                                                                p.setRegSyncNeeded(true);
                                                            }
                                                        }

                                                                                                           
                                                        if(rsuConfig.capNormalize != StringCapitalizationNormalizer.NONE){
                                                            Boolean update = false;
                                                            String f = rsuConfig.capNormalize.normalize(p.getFirstName());
                                                            if (!f.equals(p.getFirstName())) {
                                                                update = true;
                                                                logger.trace("Normalizing First Name: {} -> {}",p.getFirstName(),f);
                                                                p.setFirstName(f);
                                                            }

                                                            String m = rsuConfig.capNormalize.normalize(p.getMiddleName());
                                                            if (!m.equals(p.getMiddleName())) {
                                                                update = true;
                                                                logger.trace("Normalizing Middle Name: {} -> {}",p.getFirstName(),f);
                                                                p.setMiddleName(m);
                                                            }

                                                            String l = rsuConfig.capNormalize.normalize(p.getLastName());
                                                            if (!l.equals(p.getLastName())) {
                                                                update = true;
                                                                logger.trace("Normalizing Last Name: {} -> {}",p.getFirstName(),f);
                                                                p.setLastName(l);
                                                            }

                                                            String c = rsuConfig.capNormalize.normalize(p.getCity());
                                                            if (!c.equals(p.getCity())){
                                                                update = true;
                                                                logger.trace("Normalizing City: {} -> {}",p.getFirstName(),f);
                                                                p.setCity(c);
                                                            }

                                                            if (update) {
                                                                p.setRegSyncNeeded(update);
                                                            }                                                            
                                                        } 
                                                        
                                                        List<Wave> waveList = p.wavesObservableList();
                                                        if (waveList.isEmpty()) {
                                                            if (!multipleWaves) p.setWaves(defaultWave);
                                                            else p.setWaves(partDAO.getWaveByBib(p.getBib()));
                                                        } else {
                                                            // merge / replace time
                                                            logger.trace("syncFromRSU: Participant {} is already registered for {} other event(s)!",p.fullNameProperty().getValue(),waveList.size());
                                                            
                                                            if (!multipleWaves) {
                                                                logger.trace("The new event does NOT have multiple waves...");
                                                                if (!waveList.contains(defaultWave)) {
                                                                    logger.debug("Adding participant to the {} event",defaultWave.getRace().getRaceName());
                                                                    p.addWave(defaultWave);
                                                                } else logger.debug("Participant was already in this wave!");
                                                            } else {
                                                                logger.trace("The new event DOES have multiple waves....");
                                                                // This kinda sucks....
                                                                // TODO: Fix this mess to properly move folks from one wave to the next
                                                                Map<Race,Wave> existingRaceWaveMap = new HashMap();
                                                                waveList.forEach(w -> existingRaceWaveMap.put(w.getRace(),w)); 
                                                            
                                                                Map<Race,Wave> raceWaveMap = new HashMap();
                                                                partDAO.getWaveByBib(p.getBib()).forEach(w -> raceWaveMap.put(w.getRace(),w));
                                                                
                                                                if (existingRaceWaveMap.containsKey(race)){
                                                                    Wave newWave = defaultWave;
                                                                    if (raceWaveMap.containsKey(race)) newWave = raceWaveMap.get(race);
                                                                    
                                                                    if (newWave != existingRaceWaveMap.get(race)) {
                                                                        waveList.remove(existingRaceWaveMap.get(race));
                                                                        waveList.add(newWave);
                                                                        p.setWaves(waveList);
                                                                    }
                                                                } else { 
                                                                    if (raceWaveMap.containsKey(race)) waveList.add(raceWaveMap.get(race));
                                                                    else waveList.add(defaultWave);
                                                                    p.setWaves(waveList);
                                                                }
                                                            }
                                                        }
                                                        
                                                        p.getRegID2RaceIDDMap().put(rsuReg.optIntegerObject("registration_id"),event);
                                                           
                                                        
                                                        platformDone.countDown();
                                                    });
                                                    
                                                    platformDone.await();
                                                    // save
                                                    if (p.getID() > 0) partDAO.updateParticipant(p);
                                                    else partDAO.addParticipant(p);
                                                    
                                                    regIDtoParticipantMap.put(rsuReg.optIntegerObject("registration_id"), p);
                                                    userIDtoParticipantMap.put(p.getRegUserID(), p);
                                                    
                                                    updateMessage(p.fullNameProperty().getValueSafe());
                                                    
                                                }
                                                logger.debug("Found " + results.length() + " registrations\n\n");
                                            } catch (org.json.JSONException exj) {
                                                logger.error("JSON Exception: {}",exj.getMessage(),exj);
                                                regReturned = 0;
                                            }
                                            page++;
                                        } else {
                                            logger.error("Error in RSU Participant request: {} ", response.body());
                                            updateRSUKeys();
                                        }
                                    } else {
                                        logger.error("Error in RSU response: {} ", response.body());
                                    }
                                } catch (Exception ex) {
                                    logger.error("Exception in HttpClient response: ", ex);
                                }
                            } while (regReturned >= 1000);
                        } catch (Exception ex) {
                            logger.debug("RSU sync Exception: " + ex.getMessage());
                        }
                        
                        updateProgress(++eventsProcessed, eventsToProcess);
                    }
                }

//              // Removed Registrations
                // For each Event_ID that is not set to "IGNORE", download the participants
                for (Integer event: rsuConfig.eventToRaceMap.keySet()) {
                    if (!rsuConfig.eventToRaceMap.get(event).equals(-1)) { 
                        Integer regReturned = 0;
                        Integer page = 1;
                        
                        Race race = RaceDAO.getInstance().getRaceByID(rsuConfig.eventToRaceMap.get(event));
                        
                        logger.info("Getting removed registrations for {} with RSU EventID {}",race.getRaceName(), event);

                        try {
                            do {

                                StringBuilder rsuURL = new StringBuilder();
                                rsuURL.append("https://runsignup.com/Rest/race/").append(rsuConfig.rsuRaceID);
                                rsuURL.append("/removed-participants?format=json&event_id=").append(event);
                                rsuURL.append("&page=").append(page.toString()).append("&results_per_page=").append(pageSize);
                                rsuURL.append("&modified_after_timestamp=").append(rsuConfig.rsuLastSync);
                                rsuURL.append("&condensed_format=F");

                                // log it now before we tack on the key and secret
                                logger.debug("Participant request for race_id={} and event_id={}: {}", rsuConfig.rsuRaceID, event, rsuURL.toString());

                                if (rsuConfig.rsuLoginType.equals("API")) {
                                    rsuURL.append("&api_key=").append(rsuConfig.rsuKey);
                                    rsuURL.append("&api_secret=").append(rsuConfig.rsuSecret);
                                } else {
                                    updateRSUKeys();
                                    rsuURL.append("&tmp_key=").append(rsuConfig.rsuTempKey);
                                    rsuURL.append("&tmp_secret=").append(rsuConfig.rsuTempSecret);
                                }

                                HttpRequest request = HttpRequest.newBuilder()
                                        .uri(URI.create(rsuURL.toString()))
                                        .build();

                                HttpClient client = HttpClient.newHttpClient();

                                try {
                                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                                    if (response.statusCode() == 200) {
                                        if (response.body().startsWith("[{")) { // We have a json array....
                                            try {
                                                JSONArray results = new JSONArray(response.body()).getJSONObject(0).getJSONObject("event").getJSONArray("participants");
                                                logger.trace(results.toString(4));

                                                regReturned = results.length();
                                                for (int j = 0; j < results.length(); j++) {
                                                    JSONObject removedReg = results.getJSONObject(j);
                                                    Integer regID = removedReg.optIntegerObject("registration_id");
                                                    
                                                    logger.debug("Removed Registration for {}: RegID: {}", race,regID);


                                                    // Lookup existing registration
                                                    if (regIDtoParticipantMap.containsKey(regID)) {
                                                        Participant p = regIDtoParticipantMap.get(regID);
                                                        logger.trace("RSUSync: Found existing RSU RegistrationID {} for {}", regID, p.fullNameProperty().toString() );
                                                        
                                                        // Remove the participant from the race/wave
                                                        Set<Wave> waves = new HashSet();
                                                        p.wavesObservableList().forEach(w -> {
                                                            if (!Objects.equals(w.getRace().getID(), race.getID())) waves.add(w);
                                                        });
                                                        p.setWaves(waves);
                                                        
                                                        // Cleanup the regid -> wave map
                                                        p.getRegID2RaceIDDMap().remove(regID);

                                                        // if they are no longer registered for anything, delete them
                                                        if (p.getWaveIDs().isEmpty()) {
                                                            partDAO.removeParticipant(p);
                                                        } else {
                                                            partDAO.updateParticipant(p);
                                                        }
                                                    } else logger.trace("RSUSync: Unable to find existing registration with id {}",removedReg.optIntegerObject("registration_id"));

                                                    

                                                }
                                                logger.debug("Processed " + results.length() + " removed registrations");
                                            } catch (org.json.JSONException exJ) {
                                                regReturned = 0;
                                                logger.debug("JSON Exception in get removed-participants: " + exJ.getMessage());

                                            }//.getJSONObject(0);

                                            page++;
                                        } else {
                                            logger.error("Error in RSU Participant request: {} ", response.body());
                                            updateRSUKeys();
                                        }
                                    } else {
                                        logger.error("Error in RSU response: {} ", response.body());
                                    }
                                } catch (Exception ex) {
                                    logger.error("Exception in HttpClient response: ", ex);
                                }
                            } while (regReturned >= 1000);
                        } catch (Exception ex) {
                            logger.debug("RSU sync Exception: " + ex.getMessage());
                        }

                    updateProgress(++eventsProcessed, eventsToProcess);
                    }
                }


                updateMessage("Done!");
                updateProgress(1,1);

                

                // save lastRunTS
                rsuConfig.setRSULastSync(lastRunTS);
                // Save config to DB
                Session s = HibernateUtil.getSessionFactory().getCurrentSession();
                s.beginTransaction();
                s.saveOrUpdate(rsuConfig);
                s.getTransaction().commit();
                
                syncInProgress = false;
                
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException ex) {
                    // We don't really care....
                } finally {
                    updateMessage("");
                    updateProgress(0,1);
                }
                
                Platform.runLater(() -> {
                    progressBar.progressProperty().unbind();
                    progressLabel.textProperty().unbind();
                });

                return null;
            }
        };

        Platform.runLater(() -> {
            progressBar.progressProperty().bind(resyncTask.progressProperty());
            progressLabel.textProperty().bind(resyncTask.messageProperty());
        });
        

        Thread resync = new Thread(resyncTask);
        
        resync.setDaemon(true);
        resync.start();
    }

    public void syncFromRSU() {
        logger.debug("RunSignUpDAO::syncFromRSU() Start...");

        Dialog dialog = new Dialog();

        dialog.setTitle("Importing runners from RSU...");
        dialog.setHeaderText("Importing runners from RSU...");

        Label runnerName = new Label("");
        ProgressBar pb = new ProgressBar(0);

        VBox vbox = new VBox();
        vbox.setSpacing(5);
        vbox.getChildren().add(pb);
        vbox.getChildren().add(runnerName);
        vbox.setMaxWidth(Double.MAX_VALUE);
        vbox.setAlignment(Pos.CENTER);
        pb.setPrefWidth(300);
        runnerName.setPrefWidth(300);
        vbox.setPrefSize(450, 350);
        
        dialog.getDialogPane().setContent(vbox);
        
        ButtonType loginButtonType = new ButtonType("Close", ButtonData.OK_DONE);
        dialog.getDialogPane().getButtonTypes().addAll(loginButtonType);

        syncFromRSU(pb, runnerName);

        // TODO: Close after the sync is done. 
        
        dialog.showAndWait();

    }

    public Boolean syncToRSU() {
        logger.debug("Init: syncToRSU()");

        
        Boolean noErrors = true; 
        
        if (rsuConfig.getBiDiSync()) {
        
            // Look for new participants and create them
            //if (noErrors) noErrors = pushNewParticipants();

            // Update general attributes   
            noErrors = updateParticipants();
            logger.debug("RunSignUpDAO::syncToRSU() updateParticipants() returned {}",noErrors);

            // look for event adds/transfers/deletes and then handle them. 
            if (noErrors) noErrors =  swapRSUEvents();
            logger.debug("RunSignUpDAO::syncToRSU() swapRSUEvents() returned {}",noErrors);

            // Look for deleted participants and delete them
            List<Participant> toBeDeleted = new ArrayList();
            partDAO.listParticipants().forEach(p -> {
                
                if (p.getWaveIDs().isEmpty()) {
                    toBeDeleted.add(p);
                    logger.debug("Participant {} is in no races. Marking for deletion.",p.fullNameProperty().getValue());
                }
            });
            if (! toBeDeleted.isEmpty()) partDAO.blockingRemoveParticipants(toBeDeleted);
            
            // if no issues, clear the rsuSyncNeeded flag
            if (noErrors){
                partDAO.listParticipants().forEach(p -> {
                    if (p.getRegSyncNeeded()){
                        p.setRegSyncNeeded(false);
                        partDAO.updateParticipant(p);
                    }
                });
            } else {
                logger.warn("Error in pushing data to RSU!. Not clearing rsuSyncNeeded flag!");
            }
        }
        
        return noErrors;
    }
    
    public Boolean swapRSUEvents(){
        
           
        List<Registration> transfers = new ArrayList();
        Map<Integer,List<Registration>> adds = new ConcurrentHashMap();
        List<Registration> removes = new ArrayList();
        
        Boolean isOK = true; 
        
        logger.trace("Starting swapRSUEvents()...");
        // Look for folks whos registrations don't match their races
        // partDAO.listParticipants().parallelStream().forEach(p -> {
        partDAO.listParticipants().forEach(p -> {
            if (p.getRegSyncNeeded()){
                logger.debug("swapRSUEvents(): checking {}",p.fullNameProperty().getValueSafe());
                
                List<Integer> regIDsToRemove = new ArrayList();
                List<Integer> rsuEventsToAdd = new ArrayList();

                        
                
                // used to hold what races RSU thinks that we are in
                Set<Race> races = new HashSet();
                                
                // Look for events to remove
                for (Integer reg: p.getRegID2RaceIDDMap().keySet()){
                    logger.trace("Checking rsuRegID {} -> {}",reg,p.getRegID2RaceIDDMap().get(reg));
                    Integer rsuEventID = p.getRegID2RaceIDDMap().get(reg);
                    Integer pikaRaceID = rsuConfig.getRSUEventMap().get(rsuEventID);
                    Race race = raceDAO.getRaceByID(pikaRaceID);
                    races.add(race); // we use this in the "events to add" below
                    logger.debug("Checking if we are still in {}...", race.getRaceName());
                    Boolean isValid = false;
                    for(Integer w:p.getWaveIDs()) {
                        logger.trace("Checking waveID {}",w);
                        if(Objects.equals(race.getID(), raceDAO.getWaveByID(w).getRace().getID())) isValid=true;
                        logger.trace("isValid = {}", isValid);
                    }
                    if (!isValid) {
                        regIDsToRemove.add(reg);
                    }
                }
                
                // Look for events to add
                for(Integer i:p.getWaveIDs()){
                    Race race = raceDAO.getWaveByID(i).getRace();
                    logger.trace("Checking to see if we are registered for {} in RSU...",race.getRaceName());
                    if(! races.contains(race) ) {
                        if (rsuConfig.getRSUEvent(race) == null ) logger.debug("We are registered for race {} but that is not mapped to any RSU Event!",race.getRaceName());
                        else {
                                rsuEventsToAdd.add(rsuConfig.getRSUEvent(race));
                                logger.debug("We are NOT registered for {}, adding RSU event ID {} to the add list",race.getRaceName(), rsuConfig.getRSUEvent(race));
                        }
                    } else logger.trace("We are alread registered for {}",race.getRaceName());
                }
                
                
                // Registration(Integer rsuRegID, Integer newRSUEventID, Integer newPikaRaceID, Participant participant) 
                if (!regIDsToRemove.isEmpty() || !rsuEventsToAdd.isEmpty()) {
                    
                    while(!regIDsToRemove.isEmpty() && !rsuEventsToAdd.isEmpty()) {
                        Registration r = new Registration(regIDsToRemove.removeFirst(), rsuEventsToAdd.removeFirst(),null,p);
                        transfers.add(r);
                    }
                    for(Integer a: regIDsToRemove) {
                        Registration r = new Registration(a, null, null, p);
                        removes.add(r);
                    }
                    for (Integer a: rsuEventsToAdd){
                        if (!adds.containsKey(a)) adds.put(a, new ArrayList());
                        Registration r = new Registration(null, a, null, p);
                        adds.get(a).add(r);
                    }
                }
            }
        });

        logger.trace("swapRSUEvents(): participant scan finished: transfers: {} adds: {} removes: {}",transfers.size(),adds.size(),removes.size());
                
        // Process the transfers, adds, and deletes
        
        //////////////
        // Transfers
        // /Rest/Race/<raceID>/switch-participant-events
        //////////////
       
        
        if (!transfers.isEmpty()) {
            JSONArray transfersJSONArray = new JSONArray();
           
            transfers.forEach(r -> {
               JSONObject o = new JSONObject();
               o.put("registration_id", r.rsuRegID);
               o.put("new_event_id",r.newRSUEventID);
               transfersJSONArray.put(o);
            });

            JSONObject transfersPostData = new JSONObject();
            transfersPostData.put("registrations", transfersJSONArray);
            
            StringBuilder rsuURL = new StringBuilder();
            rsuURL.append("https://runsignup.com/Rest/race/").append(rsuConfig.rsuRaceID);
            rsuURL.append("/switch-participant-events?format=json");

            // log it now before we tack on the key and secret
            logger.debug("Swap Participant request for race_id={}: {}", rsuConfig.rsuRaceID, rsuURL.toString());

            if (rsuConfig.rsuLoginType.equals("API")) {
                rsuURL.append("&api_key=").append(rsuConfig.rsuKey);
                rsuURL.append("&api_secret=").append(rsuConfig.rsuSecret);
            } else {
                updateRSUKeys();
                rsuURL.append("&tmp_key=").append(rsuConfig.rsuTempKey);
                rsuURL.append("&tmp_secret=").append(rsuConfig.rsuTempSecret);
            }


            StringBuilder postString = new StringBuilder();
            postString.append("race_id=").append(rsuConfig.rsuRaceID);
            postString.append("&transfer_bibs=T&request_format=json");
            postString.append("&request=").append(transfersPostData.toString());


            logger.debug("Transfer Participant request for race_id={}\n {}", rsuConfig.rsuRaceID,postString.toString());
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(rsuURL.toString()))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(postString.toString()))
                    .build();
            request.bodyPublisher().get().toString();
            HttpClient client = HttpClient.newHttpClient();
            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 200) {
                    // TODO: Look for error response because RSU sends them with 200's :-/ 
                    logger.trace("RAW RSU switch-participant response.body(): {}",response.body());
                    // pull in the response to snag the new registration ID's
                    // and then update the participant records
                    if (response.body().startsWith("{")) { 
                        try {
                            JSONObject rsuResponse = new JSONObject(response.body());
                            logger.trace("RSU switch-participant Response: {} ",rsuResponse.toString(4));
                            if (!rsuResponse.has("participants")) {
                                logger.debug("No participants returned.");
                            } else {
                                JSONArray results = rsuResponse.getJSONArray("participants");
                                logger.trace(results.toString(4));

                                for (int j = 0; j < results.length(); j++) {
                                    JSONObject rsuReg = results.getJSONObject(j);
                                    Integer newRegID = rsuReg.getInt("registration_id");
                                    Registration r = transfers.get(j);
                                    Participant p = r.participant;
                                    p.getRegID2RaceIDDMap().remove(r.rsuRegID);
                                    p.getRegID2RaceIDDMap().put(newRegID,r.newRSUEventID);
                                    logger.debug("RSU Switch Event: Old rsuID: {} New rsuID: {} New EventID: {} Participant: {}", r.rsuRegID, newRegID, r.newRSUEventID, p.fullNameProperty().getValueSafe());
                                    partDAO.updateParticipant(p);
                                } 
                            }      
                        } catch (Exception e){
                            isOK = false;
                            logger.error("Exception in RSU participant transfer: {}",e.getMessage(),e);
                        }
                    }
                    
                    transfers.forEach(r -> {
                        r.participant.getRegID2RaceIDDMap().remove(r.rsuRegID);
                        partDAO.updateParticipant(r.participant);
                    });

                } else {
                    logger.warn("RSU error Response of {} {}",response.statusCode(),response.body());
                    isOK = false;
                    // TODO: Alert Dialog Box
                }
            } catch (Exception ex) {
                logger.warn("Exception in removeDeletedParticipants: {}", ex.getMessage(),ex);
                isOK = false;
                // TODO: Alert Dialog Box
            }

        } 
        
        
        
        
        //////////////
        // Adds
        //////////////
        Map<Integer,JSONArray> updatesMap = new HashMap();
        
        // Get any registrations that need to be synced up

        // if we have stuff to sync, upload them
        if (!adds.isEmpty()) {
            for(Integer rsuEventID:adds.keySet()){
                if (! adds.get(rsuEventID).isEmpty()) {
                    List<Registration> regs = adds.get(rsuEventID);
                    logger.debug("Adding {} participants to RSU EventID {}",regs.size(),rsuEventID);


                    for(Registration reg:regs){
                        Participant part = reg.participant;
                        JSONObject pData = new JSONObject();
                        if (part.getRegUserID() != null && part.getRegUserID() > 0) pData.put("user_id", part.getRegUserID());

                        // if the bib is blank, a dupe, or does not have any digits, send a null bib
                        if (part.getBib().isBlank() || part.getBib().startsWith("Dupe") || ! part.getBib().matches("\\d+")) pData.put("bib_num", JSONObject.NULL);
                        else pData.put("bib_num",Integer.parseInt(part.getBib().replaceAll("\\D", "")));  

                        JSONObject uData = new JSONObject();
                        uData.put("first_name",part.getFirstName());
                        
                        uData.put("last_name",part.getLastName());
                        uData.put("gender",part.getSex());
                        if (part.getMiddleName().isBlank()) uData.put("middle_name",JSONObject.NULL);
                        else uData.put("middle_name",part.getMiddleName());

                        JSONObject aData = new JSONObject();
                        aData.put("city",part.getCity());
                        aData.put("state",part.getState());
                        aData.put("country_code", part.getCountry());
                        uData.put("address",aData);
                        pData.put("user",uData);

                        pData.put("age",part.getAge());
                        if (part.getBirthday() == null || part.getBirthday().isBlank()) uData.put("dob",JSONObject.NULL);
                        else uData.put("dob",part.getBirthday());


                        if (!updatesMap.containsKey(rsuEventID)) {
                            updatesMap.put(rsuEventID, new JSONArray());
                        }
                        updatesMap.get(rsuEventID).put(pData);
                        logger.trace("update participants: Event id: {} pData: {}",rsuEventID,pData.toString(4));
                    }
                }
            }


            if (updatesMap.isEmpty()) {
                logger.debug("RSUSync: No pending uploads");
            } else {
                for(Integer event: updatesMap.keySet()){
                    StringBuilder rsuURL = new StringBuilder();
                    rsuURL.append("https://runsignup.com/Rest/race/").append(rsuConfig.rsuRaceID);
                    rsuURL.append("/participants?format=json");

                    // log it now before we tack on the key and secret
                    logger.debug("Add participant request for race_id={} and event_id={}: {}", rsuConfig.rsuRaceID, event, rsuURL.toString());

                    if (rsuConfig.rsuLoginType.equals("API")) {
                        rsuURL.append("&api_key=").append(rsuConfig.rsuKey);
                        rsuURL.append("&api_secret=").append(rsuConfig.rsuSecret);
                    } else {
                        updateRSUKeys();
                        rsuURL.append("&tmp_key=").append(rsuConfig.rsuTempKey);
                        rsuURL.append("&tmp_secret=").append(rsuConfig.rsuTempSecret);
                    }


                    JSONObject postData = new JSONObject();
                    postData.put("participants", updatesMap.get(event));

                    StringBuilder postString = new StringBuilder();
                    postString.append("event_id=").append(event);
                    postString.append("&restrict_potential_dup=F&clear_null_corrals=F&request_format=json");
                    postString.append("&request=").append(postData.toString());


                    logger.debug("Update Participant request for race_id={} and event_id={}: {} \n {}", rsuConfig.rsuRaceID, event ,postString.toString());
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create(rsuURL.toString()))
                            .header("Content-Type", "application/x-www-form-urlencoded")
                            .POST(HttpRequest.BodyPublishers.ofString(postString.toString()))
                            .build();
                    request.bodyPublisher().get().toString();
                    HttpClient client = HttpClient.newHttpClient();
                    try {
                        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                        if (response.statusCode() == 200) {
                            logger.debug("RSU add Participants Response: {}",response.body());
                            JSONObject rsuResponse = new JSONObject(response.body());
                            
                            if (rsuResponse.has("participants")) {
                                JSONArray adds_response_array = rsuResponse.getJSONArray("participants");

                                List<Registration> a = adds.get(event);
                                if (a.size() == adds_response_array.length()) {
                                    for (int j = 0; j < adds_response_array.length(); j++) {
                                        JSONObject rsuReg = adds_response_array.getJSONObject(j);
                                        a.get(j).participant.getRegID2RaceIDDMap().put(rsuReg.getInt("registration_id"), event);
                                        partDAO.updateParticipant(a.get(j).participant);
                                    }
                                } else {
                                    logger.error("Issue in adding participants to event: Add list size does not equal response! {} adds and {} returned!",a.size(),adds_response_array.length());
                                }
                            } else {
                                logger.debug("Error in RSU Response, looking for dup regID matches");
                                if (rsuResponse.has("error_details")) {
                                    JSONArray rsuError = rsuResponse.getJSONArray("error_details");
                                    List<Registration> a = adds.get(event);
                                    for (int j = 0; j < rsuError.length(); j++) {
                                        JSONObject rsuReg = rsuError.getJSONObject(j);
                                        if (rsuReg.getInt("error_code") == 517) { // reg id match
                                            logger.debug("We have a regID match! Adjusting...");
                                            a.get(j).participant.getRegID2RaceIDDMap().put(rsuReg.getInt("detailed_desc"), event);
                                            a.get(j).participant.setRegSyncNeeded(false);
                                            partDAO.updateParticipant(a.get(j).participant);
                                        }
                                    }
                                }
                            }
                        } else {
                            logger.warn("RSU error Response of {} {}",response.statusCode(),response.body());
                            isOK = false;
                        }
                    } catch (Exception ex) {
                        logger.warn("Exception in updateParticipants: {}",ex.getMessage(),ex);
                        isOK = false;
                    }
                }
            }
        }
        
        //////////////
        // Removes
        //////////////
        if (! removes.isEmpty()) {
            
            JSONArray removesJSONArray = new JSONArray();
            removes.forEach(r -> {
                JSONObject o = new JSONObject();
                o.put("registration_id", r.rsuRegID);
                removesJSONArray.put(o);
            });

            JSONObject removesPostData = new JSONObject();
            removesPostData.put("registrations", removesJSONArray);

        

            StringBuilder rsuURL = new StringBuilder();
            rsuURL.append("https://runsignup.com/Rest/race/").append(rsuConfig.rsuRaceID);
            rsuURL.append("/delete-participants?format=json");

            // log it now before we tack on the key and secret
            logger.debug("Delete Participant request for race_id={}: {}", rsuConfig.rsuRaceID, rsuURL.toString());

            if (rsuConfig.rsuLoginType.equals("API")) {
                rsuURL.append("&api_key=").append(rsuConfig.rsuKey);
                rsuURL.append("&api_secret=").append(rsuConfig.rsuSecret);
            } else {
                updateRSUKeys();
                rsuURL.append("&tmp_key=").append(rsuConfig.rsuTempKey);
                rsuURL.append("&tmp_secret=").append(rsuConfig.rsuTempSecret);
            }


            StringBuilder postString = new StringBuilder();
            postString.append("race_id=").append(rsuConfig.rsuRaceID);
            postString.append("&transfer_bibs=T&request_format=json");
            postString.append("&request=").append(removesPostData.toString());


            logger.debug("Delete Participant request for race_id={}\n {}", rsuConfig.rsuRaceID,postString.toString());
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(rsuURL.toString()))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(postString.toString()))
                    .build();
            request.bodyPublisher().get().toString();
            HttpClient client = HttpClient.newHttpClient();
            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 200) {
                    // TODO: Look for error response because RSU sends them with 200's :-/ 
                    logger.trace("RSU delete-participants Response: {}",response.body());
                    
                    removes.forEach(r -> {
                        r.participant.getRegID2RaceIDDMap().remove(r.rsuRegID);
                        partDAO.updateParticipant(r.participant);
                    });

                } else {
                    logger.warn("RSU error Response of {} {}",response.statusCode(),response.body());
                    // TODO: Alert Dialog Box
                    isOK = false;
                }
            } catch (Exception ex) {
                logger.warn("Exception in removeDeletedParticipants: {}", ex.getMessage(),ex);
                // TODO: Alert Dialog Box
                isOK = false;
            }

        }            
                    
        logger.trace("swapRSUEvents complete. ");
        return isOK;
        
    }
    
    // NOTE: This works but is very dangerous. 
    // We should popup a notice when folks try to remove a participant
    // that they need to go to RSU to do it. 
    
//    public List<Participant> removeDeletedParticipants(){
//        List<Participant> removedParticipants = new ArrayList();
//        
//        for(Participant p:ParticipantDAO.getInstance().listParticipants()) {
//            if (p.getRegSyncNeeded() && p.wavesObservableList().isEmpty()){
//                logger.debug("RSU: removing {} ({})",p.fullNameProperty().toString(),p.getRegID2RaceIDDMap());
//                
//                
//                JSONArray removesJSONArray = new JSONArray();
//                p.getRegID2RaceIDDMap().keySet().forEach(r -> {
//                    JSONObject o = new JSONObject();
//                    o.put("registration_id", r);
//                    removesJSONArray.put(o);
//                });
//                
//                JSONObject removesPostData = new JSONObject();
//                removesPostData.put("registrations", removesJSONArray);
//                
//                
//                if (removesPostData.length()> 0) {
//                    
//                    StringBuilder rsuURL = new StringBuilder();
//                    rsuURL.append("https://runsignup.com/Rest/race/").append(rsuConfig.rsuRaceID);
//                    rsuURL.append("/delete-participants?format=json");
//
//                    // log it now before we tack on the key and secret
//                    logger.debug("Remove Participant request for race_id={}: {}", rsuConfig.rsuRaceID, rsuURL.toString());
//
//                    if (rsuConfig.rsuLoginType.equals("API")) {
//                        rsuURL.append("&api_key=").append(rsuConfig.rsuKey);
//                        rsuURL.append("&api_secret=").append(rsuConfig.rsuSecret);
//                    } else {
//                        updateRSUKeys();
//                        rsuURL.append("&tmp_key=").append(rsuConfig.rsuTempKey);
//                        rsuURL.append("&tmp_secret=").append(rsuConfig.rsuTempSecret);
//                    }
//
//
//                    StringBuilder postString = new StringBuilder();
//                    postString.append("race_id=").append(rsuConfig.rsuRaceID);
//                    postString.append("&allow_non_imports=T&request_format=json");
//                    postString.append("&request=").append(removesPostData.toString());
//
//
//                    logger.debug("Delete Participant request for race_id={}\n {}", rsuConfig.rsuRaceID,postString.toString());
//                    HttpRequest request = HttpRequest.newBuilder()
//                            .uri(URI.create(rsuURL.toString()))
//                            .header("Content-Type", "application/x-www-form-urlencoded")
//                            .POST(HttpRequest.BodyPublishers.ofString(postString.toString()))
//                            .build();
//                    request.bodyPublisher().get().toString();
//                    HttpClient client = HttpClient.newHttpClient();
//                    try {
//                        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
//
//                        if (response.statusCode() == 200) {
//                            // TODO: Look for error response because RSU sends them with 200's :-/ 
//                            logger.debug("RSU Response: {}",response.body());
//                            logger.debug("RSU: Removed {} ({})",p.fullNameProperty().getValueSafe(),p.getRegID2RaceIDDMap());
//
//                            removedParticipants.add(p);
//                        } else {
//                            logger.warn("RSU error Response of {} {}",response.statusCode(),response.body());
//                            // TODO: Alert Dialog Box
//                        }
//                    } catch (Exception ex) {
//                        logger.warn("Exception in removeDeletedParticipants: {}", ex.getMessage(),ex);
//                        // TODO: Alert Dialog Box
//                    }
//                    
//                }            
//            }
//        }
//        
//        
//        return removedParticipants;
//        
//    }
    
    public Boolean updateParticipants(){
        logger.trace("Entering RunSignUpDAO::updateParticipants()...");
        ParticipantDAO pDAO = ParticipantDAO.getInstance();
        //RaceDAO rDAO = RaceDAO.getInstance();
        
        BooleanProperty noIssues = new SimpleBooleanProperty(true);
        
        Map<Integer,JSONArray> updatesMap = new HashMap();
        
        // Get any registrations that need to be synced up

        // if we have stuff to sync, upload them
        pDAO.listParticipants().forEach(part -> { 
            if (part.getRegSyncNeeded()) { 
                logger.debug("Syncing {} to RSU with {} registrations...",part.fullNameProperty().getValueSafe(), part.getRegID2RaceIDDMap().size());
                // Invert the regID -> eventID map
                //Map<Integer,Integer> event2RegMap = new HashMap();
                //part.getRegID2RaceIDDMap().keySet().forEach(reg -> event2RegMap.put(part.getRegID2RaceIDDMap().get(reg), reg));

                for(Integer regID:part.getRegID2RaceIDDMap().keySet()){
                    JSONObject pData = new JSONObject();
                    pData.put("registration_id", regID);

                    // if the bib is blank, a dupe, or does not have any digits, send a null bib
                    if (part.getBib().isBlank() || part.getBib().startsWith("Dupe") || ! part.getBib().matches("\\d+")) pData.put("bib_num", JSONObject.NULL);
                    else pData.put("bib_num",Integer.parseInt(part.getBib().replaceAll("\\D", "")));  

                    JSONObject uData = new JSONObject();
                    uData.put("first_name",part.getFirstName());
                    uData.put("last_name",part.getLastName());
                    if (part.getMiddleName().isBlank()) uData.put("middle_name",JSONObject.NULL);
                    else uData.put("middle_name",part.getMiddleName());
                    uData.put("gender",part.getSex());

                    JSONObject aData = new JSONObject();
                    aData.put("city",part.getCity());
                    aData.put("state",part.getState());
                    aData.put("country_code", part.getCountry());
                    uData.put("address",aData);
                    pData.put("user",uData);

                    pData.put("age",part.getAge());
                    if (part.getBirthday() == null || part.getBirthday().isBlank()) uData.put("dob",JSONObject.NULL);
                    else uData.put("dob",part.getBirthday());


                    Integer eventID = part.getRegID2RaceIDDMap().get(regID);
                    if (!updatesMap.containsKey(eventID)) {
                        updatesMap.put(eventID, new JSONArray());
                        //updatedParticipantsMap.put(eventID, new ArrayList());
                    }
                    updatesMap.get(eventID).put(pData);
                    logger.trace("update participants: Event id: {} pData: {}",eventID,pData.toString(4));
                    //updatedParticipantsMap.get(eventID).add(part);
                }
            }
        });
       

        if (updatesMap.isEmpty()) {
            logger.debug("RSUSync: No pending uploads");
        } else {
            for(Integer event: updatesMap.keySet()){
                StringBuilder rsuURL = new StringBuilder();
                rsuURL.append("https://runsignup.com/Rest/race/").append(rsuConfig.rsuRaceID);
                rsuURL.append("/participants?format=json");

                // log it now before we tack on the key and secret
                logger.debug("Participant request for race_id={} and event_id={}: {}", rsuConfig.rsuRaceID, event, rsuURL.toString());

                if (rsuConfig.rsuLoginType.equals("API")) {
                    rsuURL.append("&api_key=").append(rsuConfig.rsuKey);
                    rsuURL.append("&api_secret=").append(rsuConfig.rsuSecret);
                } else {
                    updateRSUKeys();
                    rsuURL.append("&tmp_key=").append(rsuConfig.rsuTempKey);
                    rsuURL.append("&tmp_secret=").append(rsuConfig.rsuTempSecret);
                }


                JSONObject postData = new JSONObject();
                postData.put("participants", updatesMap.get(event));

                StringBuilder postString = new StringBuilder();
                postString.append("event_id=").append(event);
                postString.append("&restrict_potential_dup=T&clear_null_corrals=F&request_format=json");
                postString.append("&request=").append(postData.toString());


                logger.debug("Update Participant request for race_id={} and event_id={}: {} \n {}", rsuConfig.rsuRaceID, event ,postString.toString());
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(rsuURL.toString()))
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .POST(HttpRequest.BodyPublishers.ofString(postString.toString()))
                        .build();
                request.bodyPublisher().get().toString();
                HttpClient client = HttpClient.newHttpClient();
                try {
                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                    if (response.statusCode() == 200) {
                        // TODO: Catch restrict_potential_dup issues 
                        logger.debug("RSU updateParticipants Response: {}",response.body());
                    } else {
                        logger.warn("RSU error Response of {} {}",response.statusCode(),response.body());
                    }
                } catch (Exception ex) {
                    logger.warn("Exception in updateParticipants: {}",ex.getMessage(),ex);
                    noIssues.set(false);
                }
            }
        }
            
       
        
//        // if no issues, clear the rsuSyncNeeded flag
//        
//        if (noIssues.get()){
//            pDAO.listParticipants().forEach(p -> {
//                if (p.getRegSyncNeeded()){
//                    p.setRegSyncNeeded(false);
//                    pDAO.updateParticipant(p);
//                }
//            });
//        } else {
//            logger.warn("Error in pushing data to RSU!. Not clearing rsuSyncNeeded flag!");
//        }
        
        return noIssues.getValue();

    }

    public void showSetupWizard() {
        
        // pre-wizzard warning
        if (!partDAO.listParticipants().isEmpty()){
            Alert alert = new Alert(AlertType.CONFIRMATION);
            alert.setTitle("Existing Participants");
            alert.setHeaderText("Existing Participants Found!");
            alert.setContentText("The RunSignUp Wizzard will remove all existing participants before syncing from RSU. This action cannot be undone. Are you sure you want to do this?");

            Optional<ButtonType> result = alert.showAndWait();
            if (result.get() != ButtonType.OK){
                return;
            }
        }
        
        
        // Wizard flow:
        // Page 1: RSU Login
        // Page 2: RSU Race Selection
        // Page 3: Map RSU Event -> PikaTimer Race
        // page 5: Options
        //         --String Normalization
        //         --Default Race -> RSU Event
        
        // TODO: 
        // Page 4: Map RSU Attributes to custom attributes
        // Page 5: Options 
        //          --City Mapping 
        
        // Page 6: Import / Finish

//        // Default RSU -> PikaTimer user attributes
//        Map<String, String> defaultAtrributeMappings = new HashMap();
//        defaultAtrributeMappings.put("FirstName", "First_Name");
//        defaultAtrributeMappings.put("MiddleName", "Middle_Name");
//        defaultAtrributeMappings.put("LastName", "Last_Name");
//        defaultAtrributeMappings.put("Sex", "Gender");
//        defaultAtrributeMappings.put("Age", "Age");
//        defaultAtrributeMappings.put("DateOfBirth", "Date_of_Birth");
//        defaultAtrributeMappings.put("City", "City");
//        defaultAtrributeMappings.put("St", "State");
//        defaultAtrributeMappings.put("Country", "Country");
//        defaultAtrributeMappings.put("E-Mail", "EMail");
//        defaultAtrributeMappings.put("Anonymous", "isAnonymous");
//        defaultAtrributeMappings.put("Swag", "Giveaway");
//        defaultAtrributeMappings.put("Bib", "Bib");
//        defaultAtrributeMappings.put("RegID", "Registration_ID");
        // Global prefs
        PikaPreferences pikaPrefs = PikaPreferences.getInstance();

        // Existing settings
        RSUConfig rsuConf = getRSUConfig();

        // Wizard variables
        final Map<Integer, Race> eventMap = new HashMap();
        //final Map<String, String> attributeMap = new HashMap();
        final Map<String, String> setupData = new HashMap();

        // pre-fill the setupData from the existing config or pikaPrefs
        // RSU Username
        if (rsuConf.rsuKey != null) {
            setupData.put("rsuKey", rsuConf.rsuKey);
        } else {
            setupData.put("rsuKey", pikaPrefs.get("rsuKey", ""));
        }

        // RSU Password
        if (rsuConf.rsuSecret != null) {
            setupData.put("rsuSecret", rsuConf.rsuSecret);
        } else {
            setupData.put("rsuSecret", pikaPrefs.getObfuscated("rsuSecret"));
        }

        // RSU Login Type
        if (rsuConf.rsuLoginType != null) {
            setupData.put("rsuLoginType", rsuConf.rsuLoginType);
        } else {
            setupData.put("rsuLoginType", pikaPrefs.get("rsuLoginType", ""));
        }

        // Event Date
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        setupData.put("eventDate", Event.getInstance().getLocalEventDate().format(formatter));

        // RSU RaceID
        if (rsuConf.rsuRaceID != null) {
            setupData.put("rsuRaceID", rsuConf.rsuRaceID.toString());
        }

        // RSU Event -> Pika Event map
        if (rsuConf.eventToRaceMap != null) {
            rsuConf.eventToRaceMap.keySet().forEach(k -> {
                setupData.put(k.toString(), rsuConf.eventToRaceMap.get(k).toString());
            });
        }
        
        // String Normalization
        setupData.put("stringNormalize", rsuConf.capNormalize.name());
        
        // Bidirectional Sync
        setupData.put("bidiSync", rsuConf.bidiSync.toString());
        
        // Pika Event -> RSU Event map
        // TODO

        List<WizardPane> wizardPanes = new ArrayList();

        Wizard wizard = new Wizard();
        wizard.setTitle("Setup RunSignUp Sync");
        
        /////////////////////////////////
        //
        // Wizard Pane 0: Existing Participant Warning
        // 


        /////////////////////////////////
        //
        // Wizard Pane 1: RSU Login Information
        // 
        // Username, password
        // onExit, do a login and stash the temp key and secret
        BooleanProperty pane1OkayToGo = new SimpleBooleanProperty(false);
        int row = 0;

        GridPane rsuLoginGrid = new GridPane();
        rsuLoginGrid.setVgap(10);
        rsuLoginGrid.setHgap(10);

        rsuLoginGrid.add(new Label("Login Method:"), 0, row);
        ComboBox<String> rsuLoginTypeComboBox = new ComboBox<>();
        rsuLoginTypeComboBox.getItems().addAll("API Key/Secret (v1)","API Key/Secret (v2)");
        if (setupData.get("rsuLoginType").equals("API")) {
            rsuLoginTypeComboBox.getSelectionModel().select("API Key/Secret (v1)");
        } else {
            rsuLoginTypeComboBox.getSelectionModel().select("API Key/Secret (v2)");
        }
        GridPane.setHgrow(rsuLoginTypeComboBox, Priority.ALWAYS);
        rsuLoginGrid.add(rsuLoginTypeComboBox, 1, row++);

        rsuLoginGrid.add(new Label("Key:"), 0, row);
        TextField rsuUsernameTextField = createTextField("rsuKey");
        rsuUsernameTextField.setText(setupData.get("rsuKey"));
        GridPane.setHgrow(rsuUsernameTextField, Priority.ALWAYS);
        rsuLoginGrid.add(rsuUsernameTextField, 1, row++);

        rsuLoginGrid.add(new Label("Secret:"), 0, row);
        PasswordField rsuPasswordTextField = new PasswordField();
        GridPane.setHgrow(rsuPasswordTextField, Priority.ALWAYS);
        rsuPasswordTextField.setText(setupData.get("rsuSecret"));
        GridPane.setHgrow(rsuPasswordTextField, Priority.ALWAYS);
        rsuLoginGrid.add(rsuPasswordTextField, 1, row++);

        rsuLoginGrid.add(new Label("Status:"), 0, row);
        Button validateButton = new Button("Validate");
        Label loginSuccessLabel = new Label("Unchecked");
        Pane loginSpring = new Pane();
        HBox.setHgrow(loginSpring, Priority.ALWAYS);
        HBox validateHBox = new HBox(loginSuccessLabel, loginSpring, validateButton);
        validateHBox.setSpacing(4);
        validateHBox.setAlignment(Pos.CENTER_LEFT);
        rsuLoginGrid.add(validateHBox, 1, row++);

        // If the username or password fields change, force a re-validation
        rsuUsernameTextField.textProperty().addListener((observable, oldValue, newValue) -> {
            pane1OkayToGo.setValue(false);
        });
        rsuPasswordTextField.textProperty().addListener((observable, oldValue, newValue) -> {
            pane1OkayToGo.setValue(false);
        });

        validateButton.setOnAction((e) -> {

            // Are we using an api_key/secret or a username / password
            if (rsuLoginTypeComboBox.getSelectionModel().getSelectedItem().contains("Username")) {
                StringBuilder postData = new StringBuilder();
                postData.append("email=");
                postData.append(URLEncoder.encode(rsuUsernameTextField.getText(), StandardCharsets.UTF_8));
                postData.append("&password=");
                postData.append(URLEncoder.encode(rsuPasswordTextField.getText(), StandardCharsets.UTF_8));
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create("https://runsignup.com/Rest/login?format=json&supports_nb=T"))
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .POST(HttpRequest.BodyPublishers.ofString(postData.toString()))
                        .build();

                HttpClient client = HttpClient.newHttpClient();

                try {
                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                    if (response.statusCode() == 200) {
                        JSONObject rsuResponse = new JSONObject(response.body());
                        if (rsuResponse.has("tmp_key")) {
                            pane1OkayToGo.setValue(true);
                            loginSuccessLabel.setText("Valid");
                            logger.debug("RSU Response: {}", response.body());

                            setupData.put("rsuLoginType", "PASSWORD");
                            setupData.put("rsuKey", rsuUsernameTextField.getText());
                            setupData.put("rsuSecret", rsuPasswordTextField.getText());
                            setupData.put("rsuTempKey", rsuResponse.getString("tmp_key"));
                            setupData.put("rsuTempSecret", rsuResponse.getString("tmp_secret"));
                            logger.debug(" RSU Temp Key/Secret: {} / {}", rsuResponse.get("tmp_key"), rsuResponse.get("tmp_secret"));
                        } else {
                            logger.error("Error in RSU Login: {} ", response.body());
                            pane1OkayToGo.setValue(false);
                            loginSuccessLabel.setText("Invalid Username or Password!");
                        }
                    } else {
                        logger.error("Error in RSU Login: {} ", response.body());
                        pane1OkayToGo.setValue(false);
                        loginSuccessLabel.setText("Invalid Username or Password!");
                    }

                } catch (Exception ex) {
                    logger.error("Exception in HttpClient response: ", ex);
                    pane1OkayToGo.setValue(false);
                    loginSuccessLabel.setText("Error in RSU Login Request");
                }
            } else {
                // There is no login equivalent for the key/secret
                // so we will just make a call to /Rest/races/ and
                // see how many we get back
                // get the list of from RSU
                // https://runsignup.com/Rest/races?
                // tmp_key=KEY&tmp_secret=SECRET
                // &format=json&include_event_days=F&page=1&results_per_page=50&sort=name+ASC
                // &start_date=2024-05-27&end_date=2024-05-27

                StringBuilder requestURL = new StringBuilder();
                requestURL.append("https://runsignup.com/Rest/races");
                requestURL.append("?api_key=").append(rsuUsernameTextField.getText());
                requestURL.append("&api_secret=").append(rsuPasswordTextField.getText());
                requestURL.append("&format=json").append("&include_event_days=T&only_partner_races=T");
                requestURL.append("&page=1&results_per_page=50&sort=name+ASC");
                requestURL.append("&start_date=").append(setupData.get("eventDate"));
                requestURL.append("&end_date=").append(setupData.get("eventDate"));

                logger.debug("RSU Get Races Request URL: {}", requestURL.toString());

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(requestURL.toString()))
                        .build();

                HttpClient client = HttpClient.newHttpClient();

                try {
                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    logger.debug("RSU Response: {} ", response.body());

                    if (response.statusCode() == 200) {
                        JSONObject rsuResponse = new JSONObject(response.body());
                        if (rsuResponse.has("races")) {
                            int responseSize = rsuResponse.getJSONArray("races").length();
                            if (responseSize > 0 && responseSize <= 50) {
                                pane1OkayToGo.setValue(true);
                                loginSuccessLabel.setText("Valid");
                                setupData.put("rsuLoginType", "API");
                                setupData.put("rsuKey", rsuUsernameTextField.getText());
                                setupData.put("rsuSecret", rsuPasswordTextField.getText());
                            } else {
                                loginSuccessLabel.setText("Invalid API Secret / Key: No races for " + setupData.get("eventDate"));
                            }
                        } else {
                            logger.error("Error in RSU Login: {} ", response.body());
                            loginSuccessLabel.setText("Invalid API Secret / Key");
                        }
                    } else {
                        logger.error("Error in RSU Login: {} ", response.body());
                        loginSuccessLabel.setText("Error in connecting to RunSignUp");
                    }
                } catch (Exception ex) {
                    logger.error("Exception in HttpClient response: ", ex);
                }
            }

        });

        final WizardPane rsuLoginWizardPane = new WizardPane() {
            @Override
            public void onEnteringPage(Wizard wizard) {
                wizard.invalidProperty().bind(pane1OkayToGo.not());
            }

            @Override
            public void onExitingPage(Wizard wizard) {
                wizard.invalidProperty().unbind();
            }
        };
        rsuLoginWizardPane.setHeaderText("RunSignUp Login");
        rsuLoginWizardPane.setContent(rsuLoginGrid);
        rsuLoginWizardPane.getStylesheets().removeFirst();
        wizardPanes.add(rsuLoginWizardPane);

        /////////////////////////////////
        // 
        // Wizard Pane 2: Get list of races from RSU 
        // 
        // Prompt the user to select the Race. Snag the race_event_days_id that is between the start_date and end_date for the rsuEvent
        row = 0;

        GridPane rsuRaceListGrid = new GridPane();
        rsuRaceListGrid.setVgap(10);
        rsuRaceListGrid.setHgap(10);

        record rsuRace(String name, Integer raceID, JSONObject details) {

            @Override
            public String toString() {
                return name;
            }
        }

        ObservableList<rsuRace> raceList = FXCollections.observableArrayList();
        ListView<rsuRace> raceListView = new ListView(raceList);
        raceListView.getSelectionModel().setSelectionMode(SelectionMode.SINGLE);

        raceListView.setPrefHeight(250);
        raceListView.setMinHeight(250);
        raceListView.setMaxWidth(Double.MAX_VALUE);
        GridPane.setHgrow(raceListView, Priority.ALWAYS);

        rsuRaceListGrid.add(raceListView, 0, row);

        setupData.put("race_event_days_id", "0");
        raceListView.getSelectionModel().selectedItemProperty().addListener((observable, oldValue, newValue) -> {
            if (newValue == null) {
                return;
            }
            LocalDate raceDate = Event.getInstance().getLocalEventDate();
            newValue.details.getJSONArray("race_event_days").forEach((r) -> {
                if (r instanceof JSONObject eventDays) {
                    LocalDate eventStart = LocalDate.parse(eventDays.getString("start_date"), DateTimeFormatter.ofPattern("M/d/yyyy 00:00"));
                    LocalDate eventEnd = LocalDate.parse(eventDays.getString("end_date"), DateTimeFormatter.ofPattern("M/d/yyyy 00:00"));
                    Integer raceEventDaysId = eventDays.getInt("race_event_days_id");
                    if (eventStart.compareTo(raceDate) <= 0 && eventEnd.compareTo(raceDate) >= 0) {
                        logger.debug("Event Days: {} ({}) is between {} and {}", raceDate, raceEventDaysId, eventStart, eventEnd);
                        setupData.put("race_event_days_id", raceEventDaysId.toString());
                        setupData.put("race_id", newValue.raceID.toString());
                    } else {
                        logger.debug("Event Days: {} ({}) is NOT between {} and {}", raceDate, raceEventDaysId, eventStart, eventEnd);
                    }
                }
            });
        });

        final WizardPane rsuRaceListWizardPane = new WizardPane() {
            @Override
            public void onEnteringPage(Wizard wizard) {
                wizard.invalidProperty().bind(raceListView.getSelectionModel().selectedItemProperty().isNull());

                raceList.clear();

                // get the list of from RSU
                // https://runsignup.com/Rest/races?
                // tmp_key=KEY&tmp_secret=SECRET
                // &format=json&include_event_days=F&page=1&results_per_page=50&sort=name+ASC
                // &start_date=2024-05-27&end_date=2024-05-27
                StringBuilder requestURL = new StringBuilder();
                requestURL.append("https://runsignup.com/Rest/races");

                if (setupData.get("rsuLoginType").equals("API")) {
                    requestURL.append("?api_key=").append(setupData.get("rsuKey"));
                    requestURL.append("&api_secret=").append(setupData.get("rsuSecret"));
                    requestURL.append("&only_partner_races=T");
                } else {
                    requestURL.append("?tmp_key=").append(setupData.get("rsuTempKey"));
                    requestURL.append("&tmp_secret=").append(setupData.get("rsuTempSecret"));
                }
                requestURL.append("&format=json").append("&include_event_days=T");
                requestURL.append("&page=1&results_per_page=50&sort=name+ASC");
                requestURL.append("&start_date=").append(setupData.get("eventDate"));
                requestURL.append("&end_date=").append(setupData.get("eventDate"));

                logger.debug("RSU Get Races Request URL: {}", requestURL.toString());

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(requestURL.toString()))
                        .build();

                HttpClient client = HttpClient.newHttpClient();

                try {
                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    logger.debug("RSU Response: {} ", response.body());

                    if (response.statusCode() == 200) {
                        JSONObject rsuResponse = new JSONObject(response.body());
                        if (rsuResponse.has("races")) {
                            rsuResponse.getJSONArray("races").forEach((r) -> {
                                if (r instanceof JSONObject rObj) {
                                    JSONObject race = rObj.getJSONObject("race"); // FFS
                                    rsuRace raceRecord = new rsuRace(URLDecoder.decode(race.getString("name"), StandardCharsets.UTF_8), race.getInt("race_id"), race);
                                    logger.debug("Found Race: {} ({})", URLDecoder.decode(race.getString("name"), StandardCharsets.UTF_8), race.getInt("race_id"));
                                    raceList.add(raceRecord);
                                }
                            });

                            if (setupData.containsKey("rsuRaceID")) {
                                Integer raceID = Integer.valueOf(setupData.get("rsuRaceID"));
                                raceList.forEach(r -> {
                                    if (raceID.equals(r.raceID)) {
                                        raceListView.getSelectionModel().select(r);
                                    }
                                });
                            }
                        } else {
                            logger.error("Error in RSU Login: {} ", response.body());
                        }
                    } else {
                        logger.error("Error in RSU Login: {} ", response.body());
                    }
                } catch (Exception ex) {
                    logger.error("Exception in HttpClient response: ", ex);
                }
            }

            @Override
            public void onExitingPage(Wizard wizard) {
                wizard.invalidProperty().unbind();
                if (!raceListView.getSelectionModel().isEmpty()) {
                    setupData.put("rsuRaceID", raceListView.getSelectionModel().getSelectedItem().raceID.toString());
                }

            }
        };
        
        rsuRaceListWizardPane.getStylesheets().removeFirst();
        wizardPanes.add(rsuRaceListWizardPane);

        rsuRaceListWizardPane.setContent(rsuRaceListGrid);
        rsuRaceListWizardPane.setHeaderText("Select RSU Race");

        /////////////////////////////////
        //
        // Page 3: Get the race details based on the race_event_days_id from #3
        // Filter event_id's based on the start_end times and create rsuEvent records
        // Show each rsuEvent and prompt the user to map each the PikaTimer Race (or set to Ignore)
        // build up the list of available questions for step 5
        
        GridPane rsuEventListGrid = new GridPane();
        rsuEventListGrid.setVgap(10);
        rsuEventListGrid.setHgap(10);

        record rsuEvent(String name, Integer eventID, SimpleObjectProperty<Race> pikaRace) {

            public rsuEvent(String name, Integer eventID, Race r) {
                this(name, eventID, new SimpleObjectProperty<>(r));
            }
            
            @Override
            public String toString(){
                return name;
            }

        }

        ObservableList<rsuEvent> eventList = FXCollections.observableArrayList();

        TableView<rsuEvent> eventTable = new TableView(eventList);
        eventTable.setEditable(true);
        eventTable.setPrefHeight(250);
        eventTable.setMinHeight(250);
        eventTable.setMaxWidth(Double.MAX_VALUE);
        GridPane.setHgrow(eventTable, Priority.ALWAYS);

        TableColumn<rsuEvent, String> eventNameTablecolumn = new TableColumn<>("RSU Event");
        eventNameTablecolumn.setCellValueFactory(cellData -> new SimpleStringProperty(cellData.getValue().name));

        TableColumn<rsuEvent, Race> pikaRaceTableColumn = new TableColumn<>("PikaTimer Event");
        pikaRaceTableColumn.setCellValueFactory(cellData -> cellData.getValue().pikaRace);
        pikaRaceTableColumn.setEditable(true);

        eventTable.getColumns().add(eventNameTablecolumn);
        eventTable.getColumns().add(pikaRaceTableColumn);

        rsuEventListGrid.add(eventTable, 0, 0);

        final WizardPane rsuEventListWizardPane = new WizardPane() {
            @Override
            public void onEnteringPage(Wizard wizard) {
                logger.debug("Start onEnteringPage() Wizard page3...");

                // List of PikaTimer Races: 
                ObservableList<Race> raceList = FXCollections.observableArrayList();
                Map<Integer, Race> raceListMap = new HashMap();
                Map<String, Race> raceListNameMap = new HashMap();

                raceDAO.listRaces().forEach(e -> {
                    raceList.add(e);
                    raceListMap.put(e.getID(), e);
                    raceListNameMap.put(e.getRaceName().toLowerCase(), e);

                });

                // and an IGNORE race
                Race dummy = new Race();
                dummy.setRaceName("Ignore");
                dummy.setID(-1);
                raceList.add(dummy);
                raceListMap.put(dummy.getID(), dummy);

//                // Read in the existing event_mapping to a map
//                Map<Integer, String> eventMap = new HashMap();
//                if (setupData.has("event_mapping")) {
//                    JSONObject map = setupData.getJSONObject("event_mapping");
//                    map.keySet().forEach(k -> {
//                        eventMap.put(Integer.valueOf(k), map.optString(k));
//                    });
//                }
                // Setup the cell factory for the rsuRace
                pikaRaceTableColumn.setCellFactory(tc -> {
                    ComboBox<Race> combo = new ComboBox<>();
                    combo.getItems().addAll(raceList);
                    TableCell<rsuEvent, Race> cell = new TableCell<rsuEvent, Race>() {
                        @Override
                        protected void updateItem(Race r, boolean empty) {
                            super.updateItem(r, empty);
                            if (empty) {
                                setGraphic(null);
                            } else {
                                combo.setValue(r);
                                setGraphic(combo);
                            }
                        }
                    };
                    combo.setOnAction(e -> {
                        // set the display name and race id code
                        tc.getTableView().getItems().get(cell.getIndex()).pikaRace.setValue(combo.getValue());
                    });
                    return cell;
                });

                eventList.clear();

                StringBuilder requestURL = new StringBuilder();
                requestURL.append("https://runsignup.com/Rest/race/");
                requestURL.append(setupData.get("race_id"));
                if (setupData.get("rsuLoginType").equals("API")) {
                    requestURL.append("?api_key=").append(setupData.get("rsuKey"));
                    requestURL.append("&api_secret=").append(setupData.get("rsuSecret"));
                } else {
                    requestURL.append("?tmp_key=").append(setupData.get("rsuTempKey"));
                    requestURL.append("&tmp_secret=").append(setupData.get("rsuTempSecret"));
                }
                requestURL.append("&format=json").append("&future_events_only=F&most_recent_events_only=F");
                requestURL.append("&race_event_days_id=").append(setupData.get("race_event_days_id"));
                requestURL.append("&include_questions=T");

                logger.debug("RSU Get Race Request URL: {}", requestURL.toString());

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(requestURL.toString()))
                        .build();

                HttpClient client = HttpClient.newHttpClient();

                try {
                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    logger.debug("RSU Response: {} ", response.body());

                    if (response.statusCode() == 200) {
                        JSONObject rsuResponse = new JSONObject(response.body());
                        if (rsuResponse.has("race")) {

                            // Get the events
                            LocalDate raceDate = Event.getInstance().getLocalEventDate();
                            rsuResponse.getJSONObject("race").getJSONArray("events").forEach((r) -> {
                                if (r instanceof JSONObject event) {
                                    LocalDate eventStart = LocalDate.parse(event.getString("start_time").replaceAll(" ..:..", ""), DateTimeFormatter.ofPattern("M/d/yyyy"));
                                    // The end_time is optional and thus can be null. So it will default to the start_date. 
                                    LocalDate eventEnd = eventStart;
                                    if (!event.isNull("end_time")) {
                                        eventEnd = LocalDate.parse(event.getString("end_time").replaceAll(" ..:..", ""), DateTimeFormatter.ofPattern("M/d/yyyy"));
                                    }
                                    if (eventStart.compareTo(raceDate) <= 0 && eventEnd.compareTo(raceDate) >= 0) {

                                        // Populate the matching race 
                                        Race race = dummy;
                                        if (setupData.containsKey(Integer.toString(event.getInt("event_id")))) {
                                            logger.debug("Found Matching raceID <-> eventID in config...");
                                            if (raceListMap.containsKey(Integer.valueOf(setupData.get(Integer.toString(event.getInt("event_id")))))) {
                                                race = raceListMap.get(Integer.valueOf(setupData.get(Integer.toString(event.getInt("event_id")))));
                                                logger.debug("Found Matching raceID <-> eventID in config: {} to {}", race.getID(), event.getInt("event_id"));
                                            } else logger.debug("Found Matching race -> event ID but NO current race matches!!! Setting to default....");
                                        } else if (raceListNameMap.containsKey(event.getString("name").toLowerCase())) {
                                            race = raceListNameMap.get(event.getString("name").toLowerCase());
                                            logger.debug("Found name match for RSU event -> Pika Race: {} -> {}", event.getString("name"), race.getRaceName());
                                        }

                                        rsuEvent eventRecord = new rsuEvent(URLDecoder.decode(event.getString("name"), StandardCharsets.UTF_8), event.getInt("event_id"), race);
                                        logger.debug("Found Event: {} ({})", URLDecoder.decode(event.getString("name"), StandardCharsets.UTF_8), event.getInt("event_id"));
                                        eventList.add(eventRecord);

                                        /*if (eventMap.containsKey(event.getInt("event_id"))) {
                                            eventRecord.pikaRace.setValue(eventMap.get(eventRecord.eventID));
                                        } */
                                    } else {
                                        logger.debug("Event {} ({}) is NOT on {}", event.getString("name"), event.getInt("event_id"), eventStart, eventEnd);
                                    }
                                }
                            });

                            // Stash the questions
                            if (rsuResponse.getJSONObject("race").has("questions")) {
                                Map<Integer, String> rsuQuestions = new HashMap();
                                rsuResponse.getJSONObject("race").getJSONArray("questions").forEach((q) -> {
                                    if (q instanceof JSONObject question) {
                                        rsuQuestions.put(question.getInt("question_id"), question.getString("question_text"));
                                    }
                                });
                                /* setupData.put("rsuQuestions", rsuQuestions); */
                            }
                        } else {
                            logger.error("Error in RSU Login: {} ", response.body());
                        }
                    } else {
                        logger.error("Error in RSU Login: {} ", response.body());
                    }
                } catch (Exception ex) {
                    logger.error("Exception in HttpClient response: ", ex);
                }
                logger.debug("End onEnteringPage() Wizard page3...");
            }

            @Override
            public void onExitingPage(Wizard wizard) {
                logger.debug("Start onExitingPage() Wizard page3...");
                wizard.invalidProperty().unbind();

                eventList.forEach(e -> {
                    setupData.put(e.eventID.toString(), e.pikaRace.getValue().getID().toString());
                    logger.debug("RSU Event -> Pika RaceID: {} -> {}", e.eventID, e.pikaRace.getValue().getID());
                });

                logger.debug("End onExitingPage() Wizard page3...");
            }
        };
        wizardPanes.add(rsuEventListWizardPane);

        rsuEventListWizardPane.setContent(rsuEventListGrid);
        rsuEventListWizardPane.setHeaderText("Map RSU Event to Pika Event");

        /////////////////////////////////
        //
        // Page 5: Options
        // 
        // 
        GridPane optionsGridPane = new GridPane();
        optionsGridPane.setVgap(10);
        optionsGridPane.setHgap(10);
        
        // BiDi Sync toggle
        ToggleSwitch bidiToggleSwitch = new ToggleSwitch("Sync back to RSU");
        // Capitalization drop down;
        ChoiceBox<StringCapitalizationNormalizer> normalizeChoiceBox = new ChoiceBox();
        
        // Table for pika race -> rsu event mappings
        
        
        record pikaRace(Race pikaRace, SimpleObjectProperty<rsuEvent> rsuEvent) {
            public pikaRace(Race race, rsuEvent event) {
                this(race, new SimpleObjectProperty<>(event));
            }
        }

        ObservableList<pikaRace> pikaRaceList = FXCollections.observableArrayList();

        TableView<pikaRace> raceTable = new TableView(pikaRaceList);
        raceTable.setEditable(true);
        raceTable.setPrefHeight(200);
        raceTable.setMinHeight(200);
        raceTable.setMaxWidth(Double.MAX_VALUE);
        GridPane.setHgrow(raceTable, Priority.ALWAYS);

        TableColumn<pikaRace, rsuEvent> raceTableRSUEventTablecolumn = new TableColumn<>("RSU Event");
        raceTableRSUEventTablecolumn.setCellValueFactory(cellData -> cellData.getValue().rsuEvent);
        raceTableRSUEventTablecolumn.setEditable(true);
        
        TableColumn<pikaRace, String> raceTablePikaRaceTableColumn = new TableColumn<>("PikaTimer Event");
        raceTablePikaRaceTableColumn.setCellValueFactory(cellData -> cellData.getValue().pikaRace.raceNameProperty());
        

        raceTable.getColumns().add(raceTablePikaRaceTableColumn);
        raceTable.getColumns().add(raceTableRSUEventTablecolumn);

        int optRow = 0;
        optionsGridPane.add(new Label("Set Default Pika Event -> RSU Event"),0,optRow);
        optRow++;
        
        optionsGridPane.add(raceTable, 0, optRow);
        optRow++;
        
        optionsGridPane.add(bidiToggleSwitch,0,optRow);
        optRow++;
        
        HBox normalizeHBox = new HBox(new Label("Normalize Names"),normalizeChoiceBox);
        normalizeHBox.setSpacing(4);
        normalizeHBox.setAlignment(Pos.CENTER_LEFT);
        optionsGridPane.add(normalizeHBox,0,optRow);

        

        
        final WizardPane optionsWizardPane = new WizardPane() {
            @Override
            public void onEnteringPage(Wizard wizard) {
                // set the normalize drop down
                normalizeChoiceBox.getItems().setAll(StringCapitalizationNormalizer.values());
                if (setupData.containsKey("stringNormalize")) 
                    normalizeChoiceBox.setValue(StringCapitalizationNormalizer.valueOf(setupData.get("stringNormalize")));
                else normalizeChoiceBox.setValue(StringCapitalizationNormalizer.TitleCase);
                
                // set the bidi sync option
                bidiToggleSwitch.selectedProperty().setValue(Boolean.valueOf(setupData.get("bidiSync")));
                
                // setup the pika -> rsu mapping table
                pikaRaceList.clear();
                // Setup the Pika -> possible RSU Event Map
                Map<Race,List<rsuEvent>> r2eMap = new HashMap();
                Map<Integer,rsuEvent> rsuEventMap = new HashMap();
                eventList.forEach(e -> {
                    if (!r2eMap.containsKey(e.pikaRace.getValue())) {
                        r2eMap.put(e.pikaRace.getValue(), new ArrayList());
                    } 
                    r2eMap.get(e.pikaRace.getValue()).add(e);    
                    rsuEventMap.put(e.eventID, e);
                    logger.trace("r2eMap add: Adding {} to {}: total: {}",e.name,e.pikaRace.getValue().getRaceName(),r2eMap.get(e.pikaRace.getValue()).size());
                });
                raceDAO.listRaces().forEach(r -> {
                    if (r2eMap.containsKey(r)) {
                        pikaRace race = new pikaRace(r,new SimpleObjectProperty<>(r2eMap.get(r).getFirst()));
                        if (rsuConf.raceToEventMap != null && rsuConf.raceToEventMap.containsKey(r.getID()) && r2eMap.get(r).contains(rsuEventMap.get(rsuConf.raceToEventMap.get(r.getID()))) ) {
                            race.rsuEvent.setValue(rsuEventMap.get(rsuConf.raceToEventMap.get(r.getID())));
                            logger.trace("Existing race -> rsu event map found: {} -> {}",r.getRaceName(),rsuEventMap.get(rsuConf.raceToEventMap.get(r.getID())).name);
                        }
                        pikaRaceList.add(race);
                    }
                });
                
                // Setup the cell factory for the rsuRace
                raceTableRSUEventTablecolumn.setCellFactory(tc -> {
                    ComboBox<rsuEvent> combo = new ComboBox<>();
                    TableCell<pikaRace, rsuEvent> cell = new TableCell<pikaRace, rsuEvent>() {
                        @Override
                        protected void updateItem(rsuEvent r, boolean empty) {
                            super.updateItem(r, empty);
                            if (empty) {
                                setGraphic(null);
                            } else {
                                combo.getItems().setAll(r2eMap.get(getTableView().getItems().get(getIndex()).pikaRace));
                                combo.setValue(r);
                                setGraphic(combo);
                            }
                        }
                    };
                    
                    combo.setOnAction(e -> {
                        tc.getTableView().getItems().get(cell.getIndex()).rsuEvent.setValue(combo.getValue());
                    });
                    return cell;
                });
                
            }
            
            @Override
            public void onExitingPage(Wizard wizard) {
                logger.debug("Start onExitingPage() Wizard optionsWizardPane...");
                wizard.invalidProperty().unbind();
                
                setupData.put("stringNormalize", normalizeChoiceBox.getSelectionModel().getSelectedItem().name());
                
                setupData.put("bidiSync",bidiToggleSwitch.selectedProperty().getValue().toString());

                pikaRaceList.forEach(r -> {
                    logger.debug(" Pika Event -> RSU Event: {} -> {}", r.pikaRace.getRaceName(), r.rsuEvent.getValue().name);
                });

                logger.debug("End onExitingPage() Wizard optionsWizardPane...");
            }
        };
        
        wizardPanes.add(optionsWizardPane);

        optionsWizardPane.setContent(optionsGridPane);
        optionsWizardPane.setHeaderText("Defaults and Options");
        
//        /////////////////////////////////
//        //
//        // Page 4: Map the RSU questions -> Pikatimer custom attributes
//        // For each registration attribute, select an RSU source (native registration field or question/givaway source if available)
//        // Set all of the panes to the same height to make this a bit nicer
//        GridPane page5Grid = new GridPane();
//        page5Grid.setVgap(10);
//        page5Grid.setHgap(10);
//
//        record regAttribute(String pprrField, StringProperty rsuField) {
//
//        }
//
//        ObservableList<regAttribute> regAttributeList = FXCollections.observableArrayList();
//
//        TableView<regAttribute> regAttributeTable = new TableView(regAttributeList);
//        regAttributeTable.setEditable(true);
//        regAttributeTable.setPrefHeight(250);
//        regAttributeTable.setMinHeight(250);
//        regAttributeTable.setMaxWidth(Double.MAX_VALUE);
//        GridPane.setHgrow(regAttributeTable, Priority.ALWAYS);
//        regAttributeTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_LAST_COLUMN);
//
//        TableColumn<regAttribute, String> pprrAttributeTablecolumn = new TableColumn<>("PPRRScore Field");
//        pprrAttributeTablecolumn.setCellValueFactory(cellData -> new SimpleStringProperty(cellData.getValue().pprrField));
//
//        TableColumn<regAttribute, String> rsuAttributeTableColumn = new TableColumn<>("RSU Attribute");
//        rsuAttributeTableColumn.setCellValueFactory(cellData -> cellData.getValue().rsuField);
//        rsuAttributeTableColumn.setEditable(true);
//
//        regAttributeTable.getColumns().add(pprrAttributeTablecolumn);
//        regAttributeTable.getColumns().add(rsuAttributeTableColumn);
//
//        page5Grid.add(regAttributeTable, 0, 0);
//
//        final WizardPane page4 = new WizardPane() {
//            @Override
//            public void onEnteringPage(Wizard wizard) {
//                logger.debug("Start onEnteringPage() Wizard page4...");
//
//                regAttributeList.clear();
//
//                // Setup the PPRRScore attributes that we need to map
//                /*
//                setupData.getJSONObject("PPRRScoreFieldList").getJSONArray("RegFields").iterator().forEachRemaining(e -> {
//                    if (e instanceof String regField) {
//                        if (!"Div".equals(regField)) {
//                            String def = defaultMappings.containsKey(regField) ? defaultMappings.get(regField) : "BLANK";
//                            logger.debug("Setting {} to {}", regField, def);
//                            regAttributeList.add(new regAttribute(regField, new SimpleStringProperty(def)));
//                        }
//                    }
//                });
//                */
//
//                // List of possible RSU fields 
//                List<String> rsuAttributesList = new ArrayList();
//
//                // Basic RSU Attributes
//                rsuAttributesList.addAll(Arrays.asList("First_Name", "Middle_Name", "Last_Name"));
//                rsuAttributesList.addAll(Arrays.asList("Gender", "Age", "Date_of_Birth", "Bib"));
//                rsuAttributesList.addAll(Arrays.asList("City", "State", "Country"));
//                rsuAttributesList.addAll(Arrays.asList("EMail", "Giveaway", "isAnonymous", "Team_Name", "Registration_ID"));
//
//                // Question Responses
//                /* 
//                if (setupData.has("rsuQuestions")) {
//                    JSONObject rsuQuestions = setupData.getJSONObject("rsuQuestions");
//                    rsuQuestions.keySet().forEach((q) -> {
//                        rsuAttributesList.add(rsuQuestions.optString(q));
//                    });
//                } */
//
//                // CatchAll for when we just dont care
//                rsuAttributesList.add("BLANK");
//
//                // Setup the cell factory for the attribute map
        ////                rsuAttributeTableColumn.setCellFactory(tc -> {
////                    ComboBox<String> combo = new ComboBox<>();
////                    combo.getItems().addAll(rsuAttributesList);
////                    TableCell<regAttribute, String> cell = new TableCell<regAttribute, String>() {
////                        @Override
////                        protected void updateItem(String reason, boolean empty) {
////                            super.updateItem(reason, empty);
////                            if (empty) {
////                                setGraphic(null);
////                            } else {
////                                combo.setValue(reason);
////                                setGraphic(combo);
////                            }
////                        }
////                    };
////                    combo.setOnAction(e -> {
////                        tc.getTableView().getItems().get(cell.getIndex()).rsuField.setValue(combo.getValue());
////                    });
////                    return cell;
////                });
//                logger.debug("End onEnteringPage() Wizard page4...");
//            }
//
//            @Override
//            public void onExitingPage(Wizard wizard) {
//                wizard.invalidProperty().unbind();
//                regAttributeList.forEach(e -> {
//                    //pprrscoreFieldMap.put(e.pprrField, e.rsuField.getValue());
//                    logger.debug(" PPRRScore Fieldlist: {} -> {}", e.pprrField, e.rsuField.getValue());
//                });
//                //setupData.put("fieldlist_mapping", pprrscoreFieldMap);
//            }
//        };
//
//        wizardPanes.add(page4);
//
//        page4.setContent(page5Grid);
//        page4.setHeaderText("Map RSU Attributes to PPRRScore Fieldlist");

        //////////////////////////////////
        //
        // Showtime....
        //
        for (WizardPane p : wizardPanes) {
            p.setMinSize(500, 400);
        }

        wizard.setFlow(new Wizard.LinearFlow(wizardPanes));

        // show wizard and wait for response
        wizard.showAndWait().ifPresent(result -> {
            if (result == ButtonType.FINISH) {

                logger.debug("setupData: {} ", setupData);

                // Save the rsu Username/Password to the global prefs
                pikaPrefs.set("rsuLoginType", setupData.get("rsuLoginType"));
                pikaPrefs.set("rsuKey", setupData.get("rsuKey"));
                pikaPrefs.setObfuscated("rsuSecret", setupData.get("rsuSecret"));

                // Save the Username/Password/LoginType to the race db
                rsuConf.rsuSecret = setupData.get("rsuSecret");
                rsuConf.rsuKey = setupData.get("rsuKey");
                rsuConf.rsuLoginType = setupData.get("rsuLoginType");

                rsuConf.rsuRaceID = Integer.valueOf(setupData.get("rsuRaceID"));
                
                rsuConfig.rsuLastSync = 0L;

                rsuConf.eventToRaceMap = new HashMap();
                setupData.keySet().forEach(k -> {
                    if (k.matches("^\\d+$")) {
                        rsuConf.eventToRaceMap.put(Integer.valueOf(k), Integer.valueOf(setupData.get(k)));
                    }
                });
                
                rsuConf.raceToEventMap = new HashMap();
                pikaRaceList.forEach(r -> {
                    logger.debug("Saving Pika Event -> RSU Event: {} -> {}", r.pikaRace.getRaceName(), r.rsuEvent.getValue().name);
                    rsuConf.raceToEventMap.put(r.pikaRace.getID(), r.rsuEvent.getValue().eventID);
                });
                
                // bidi sync
                rsuConf.setBiDiSync(Boolean.valueOf(setupData.get("bidiSync")));
                // Normalize srings
                rsuConf.setNormalizeCapitalization(StringCapitalizationNormalizer.valueOf(setupData.get("stringNormalize")));

                // Save config to DB
                Session s = HibernateUtil.getSessionFactory().getCurrentSession();
                s.beginTransaction();
                s.saveOrUpdate(rsuConf);
                s.getTransaction().commit();

                isSetup.set(true);
                
                // clear existing participants
                if (!partDAO.listParticipants().isEmpty()) partDAO.blockingClearAll();
                
                // Sync from RSU
                syncFromRSU();
            }
        });

    }

    //Utility method for the ControlsFX Wizard
    private TextField createTextField(String id) {
        TextField textField = new TextField();
        textField.setId(id);
        GridPane.setHgrow(textField, Priority.ALWAYS);
        return textField;
    }

    private void updateRSUKeys() {
        
        // If we have updated the temp keys in the last 10 minutes, just return
        Long now = Instant.now().getEpochSecond();
        if (now - rsuConfig.rsuTempTimestamp < 600) return;

        StringBuilder postData = new StringBuilder();
        postData.append("email=");
        postData.append(URLEncoder.encode(rsuConfig.rsuKey, StandardCharsets.UTF_8));
        postData.append("&password=");
        postData.append(URLEncoder.encode(rsuConfig.rsuSecret, StandardCharsets.UTF_8));
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("https://runsignup.com/Rest/login?format=json&supports_nb=T"))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(postData.toString()))
                .build();

        HttpClient client = HttpClient.newHttpClient();

        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                JSONObject rsuResponse = new JSONObject(response.body());
                if (rsuResponse.has("tmp_key")) {

                    logger.debug("updateRSUKeys -> RSU Response: {}", response.body());
                    rsuConfig.rsuTempKey = rsuResponse.getString("tmp_key");
                    rsuConfig.rsuTempSecret = rsuResponse.getString("tmp_secret");
                    rsuConfig.rsuTempTimestamp = Instant.now().getEpochSecond();
                    logger.debug(" RSU Temp Key/Secret: {} / {}", rsuResponse.get("tmp_key"), rsuResponse.get("tmp_secret"));
                } else {
                    logger.error("Error in RSU Login: {} ", response.body());
                }
            } else {
                logger.error("Error in RSU Login: {} ", response.body());
            }
        } catch (Exception ex) {
            logger.error("Exception in HttpClient response: ", ex);

        }
    }
    
    record Registration(Integer rsuRegID, Integer newRSUEventID, Integer newPikaRaceID, Participant participant) { };

}
