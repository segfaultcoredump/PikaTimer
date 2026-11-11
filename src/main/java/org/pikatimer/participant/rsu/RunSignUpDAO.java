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
package org.pikatimer.participant.rsu;

import org.pikatimer.PikaPreferences;
import org.pikatimer.event.Event;
import org.pikatimer.participant.Participant;
import org.pikatimer.participant.ParticipantDAO;
import org.pikatimer.race.Race;
import org.pikatimer.race.RaceDAO;
import org.pikatimer.race.Wave;
import org.pikatimer.util.HibernateUtil;
import org.pikatimer.util.StringCapitalizationNormalizer;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;
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
                // if we are using OAuth, make sure the access token is still good
                logger.debug("syncFromRSU: using login type {}", rsuConfig.rsuLoginType);
                if (RSUConfig.LOGIN_PASSWORD.equals(rsuConfig.rsuLoginType)) {
                    updateRSUKeys();
                } else if (RSUConfig.LOGIN_OAUTH.equals(rsuConfig.rsuLoginType)) {
                    ensureFreshToken(false);
                }
                
                
                Map<Integer,Participant> regIDtoParticipantMap = new HashMap<>();
                Map<Integer,Participant> userIDtoParticipantMap = new HashMap<>();
                
                partDAO.listParticipants().forEach(p -> {
                    // locally created participants have no RSU user id, don't map them
                    if (p.getRegUserID() != null) userIDtoParticipantMap.put(p.getRegUserID(), p);
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

                                HttpRequest request = authorizedRequest(rsuURL, currentAuth())
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
                                                                                                            
                                                    Integer rsuRegID = rsuReg.optIntegerObject("registration_id");
                                                    Integer rsuUserID = rsuReg.getJSONObject("user").optIntegerObject("user_id");
                                                    Participant p;
                                                    if (regIDtoParticipantMap.containsKey(rsuRegID)) {
                                                        p = regIDtoParticipantMap.get(rsuRegID);
                                                        logger.trace("RSUSync: Found existing RSU RegistrationID {} for {}", rsuRegID, p.fullNameProperty().getValue() );
                                                    } else if (rsuUserID != null && userIDtoParticipantMap.containsKey(rsuUserID)) {
                                                        p = userIDtoParticipantMap.get(rsuUserID);
                                                        logger.trace("RSUSync: Found existing RSU UserID {} for {}", rsuUserID, p.fullNameProperty().getValue() );
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
                                                        
                                                        // Work on a copy of the wave list. Never hand the participant's
                                                        // own wave list back to setWaves().
                                                        List<Wave> newWaves = new ArrayList<>(p.wavesObservableList());
                                                        boolean wavesChanged = false;
                                                        if (!newWaves.isEmpty()) logger.trace("syncFromRSU: Participant {} is already in {} wave(s)",p.fullNameProperty().getValue(),newWaves.size());

                                                        // Was this registration previously in a different RSU event? (moved within RSU)
                                                        Integer oldEvent = p.getRegID2RaceIDDMap().get(rsuRegID);
                                                        if (oldEvent != null && !oldEvent.equals(event)) {
                                                            Integer oldRaceID = rsuConfig.eventToRaceMap.get(oldEvent);
                                                            logger.debug("RSUSync: RegID {} for {} moved from RSU event {} to {}", rsuRegID, p.fullNameProperty().getValueSafe(), oldEvent, event);
                                                            if (oldRaceID != null && !oldRaceID.equals(-1) && !oldRaceID.equals(race.getID())) {
                                                                if (otherRegInRace(p, rsuRegID, oldRaceID)) {
                                                                    logger.debug("RSUSync: keeping the old race waves for {}, another registration still maps to race id {}", p.fullNameProperty().getValueSafe(), oldRaceID);
                                                                } else {
                                                                    logger.debug("RSUSync: removing {} from race id {}", p.fullNameProperty().getValueSafe(), oldRaceID);
                                                                    wavesChanged |= newWaves.removeIf(w -> Objects.equals(w.getRace().getID(), oldRaceID));
                                                                }
                                                            }
                                                        }

                                                        // Make sure they are in the right wave for this race
                                                        Wave targetWave = waveForRace(race, p.getBib(), defaultWave);
                                                        Optional<Wave> existingWave = newWaves.stream().filter(w -> Objects.equals(w.getRace().getID(), race.getID())).findFirst();
                                                        if (existingWave.isEmpty()) {
                                                            logger.debug("RSUSync: adding {} to {} ({})", p.fullNameProperty().getValueSafe(), race.getRaceName(), targetWave.getWaveName());
                                                            newWaves.add(targetWave);
                                                            wavesChanged = true;
                                                        } else if (multipleWaves && existingWave.get() != targetWave) {
                                                            logger.debug("RSUSync: moving {} from wave {} to {}", p.fullNameProperty().getValueSafe(), existingWave.get().getWaveName(), targetWave.getWaveName());
                                                            newWaves.remove(existingWave.get());
                                                            newWaves.add(targetWave);
                                                            wavesChanged = true;
                                                        } else logger.trace("RSUSync: participant was already in this race/wave");

                                                        if (wavesChanged) p.setWaves(newWaves);

                                                        p.getRegID2RaceIDDMap().put(rsuRegID,event);
                                                           
                                                        
                                                        platformDone.countDown();
                                                    });
                                                    
                                                    platformDone.await();
                                                    // save
                                                    if (p.getID() > 0) partDAO.updateParticipant(p);
                                                    else partDAO.addParticipant(p);
                                                    
                                                    regIDtoParticipantMap.put(rsuRegID, p);
                                                    if (p.getRegUserID() != null) userIDtoParticipantMap.put(p.getRegUserID(), p);
                                                    
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
                                            handleAuthFailure();
                                        }
                                    } else {
                                        logger.error("Error in RSU response: {} ", response.body());
                                        if (response.statusCode() == 401 || response.statusCode() == 403) handleAuthFailure();
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

                                HttpRequest request = authorizedRequest(rsuURL, currentAuth())
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

                                                        // If this registration now maps to a different event (it was moved),
                                                        // it is still active. Leave it alone.
                                                        Integer mappedEvent = p.getRegID2RaceIDDMap().get(regID);
                                                        if (mappedEvent != null && !mappedEvent.equals(event)) {
                                                            logger.debug("RSUSync: Removed RegID {} for {} is now mapped to RSU event {}, not {}. Skipping.", regID, p.fullNameProperty().getValueSafe(), mappedEvent, event);
                                                            continue;
                                                        }

                                                        // Cleanup the regid -> wave map
                                                        p.getRegID2RaceIDDMap().remove(regID);

                                                        // Remove the participant from the race/wave unless another
                                                        // registration still puts them in this race
                                                        if (otherRegInRace(p, regID, race.getID())) {
                                                            logger.debug("RSUSync: {} has another registration for {}, keeping them in the race", p.fullNameProperty().getValueSafe(), race.getRaceName());
                                                        } else {
                                                            Set<Wave> waves = new HashSet();
                                                            p.wavesObservableList().forEach(w -> {
                                                                if (!Objects.equals(w.getRace().getID(), race.getID())) waves.add(w);
                                                            });
                                                            logger.debug("RSUSync: removing {} from {}", p.fullNameProperty().getValueSafe(), race.getRaceName());
                                                            p.setWaves(waves);
                                                        }

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
                                            handleAuthFailure();
                                        }
                                    } else {
                                        logger.error("Error in RSU response: {} ", response.body());
                                        if (response.statusCode() == 401 || response.statusCode() == 403) handleAuthFailure();
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

    // Only one push to RSU at a time. Overlapping runs would submit the same
    // adds / transfers more than once.
    private final ReentrantLock syncToRSULock = new ReentrantLock();

    public Boolean syncToRSU() {
        logger.debug("Init: syncToRSU()");

        if (!syncToRSULock.tryLock()) {
            // Someone else is already pushing. Wait for them to finish and then
            // run again so that anything flagged after they started is picked up.
            logger.debug("syncToRSU(): another sync to RSU is in progress, waiting for it to finish");
            syncToRSULock.lock();
        }
        try {
            return doSyncToRSU();
        } finally {
            syncToRSULock.unlock();
        }
    }

    private Boolean doSyncToRSU() {

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
            // Only do this if everything above succeeded so we don't drop a local
            // participant whose RSU changes are still pending.
            if (noErrors) {
                List<Participant> toBeDeleted = new ArrayList();
                partDAO.listParticipants().forEach(p -> {

                    if (p.getWaveIDs().isEmpty()) {
                        toBeDeleted.add(p);
                        logger.debug("Participant {} is in no races. Marking for deletion.",p.fullNameProperty().getValue());
                    }
                });
                if (! toBeDeleted.isEmpty()) partDAO.blockingRemoveParticipants(toBeDeleted);
            } else {
                logger.debug("syncToRSU(): errors pushing to RSU, skipping the removal of participants with no races");
            }

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
                    Race race = (pikaRaceID == null || pikaRaceID.equals(-1)) ? null : raceDAO.getRaceByID(pikaRaceID);
                    if (race == null) {
                        logger.warn("swapRSUEvents(): RSU RegID {} for {} is in RSU event {} which is not mapped to a Pika race. Skipping.",
                                reg, p.fullNameProperty().getValueSafe(), rsuEventID);
                        continue;
                    }
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
                    // RSU does not permit deleting registrations via the API.
                    // Leave any unpaired registrations in place.
                    for(Integer a: regIDsToRemove) {
                        logger.warn("swapRSUEvents(): {} is no longer in the race for RSU RegID {} (event {}), but RSU registrations cannot be removed via the API. Leaving it in place.",
                                p.fullNameProperty().getValueSafe(), a, p.getRegID2RaceIDDMap().get(a));
                    }
                    for (Integer a: rsuEventsToAdd){
                        if (!adds.containsKey(a)) adds.put(a, new ArrayList());
                        Registration r = new Registration(null, a, null, p);
                        adds.get(a).add(r);
                    }
                }
            }
        });

        logger.trace("swapRSUEvents(): participant scan finished: transfers: {} adds: {}",transfers.size(),adds.size());
                
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

            StringBuilder postString = new StringBuilder();
            postString.append("race_id=").append(rsuConfig.rsuRaceID);
            postString.append("&transfer_bibs=T&request_format=json");
            postString.append("&request=").append(transfersPostData.toString());


            logger.debug("Transfer Participant request for race_id={}\n {}", rsuConfig.rsuRaceID,postString.toString());
            HttpRequest request = authorizedRequest(rsuURL, currentAuth())
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
                    // RSU returns the new registration ids in the same order as the request
                    // (the registration id changes on a switch). If anything looks off we keep
                    // the existing mappings rather than guess.
                    try {
                        JSONObject rsuResponse = new JSONObject(response.body());
                        logger.trace("RSU switch-participant Response: {} ",rsuResponse.toString(4));
                        if (rsuResponse.has("error") || rsuResponse.has("error_details")) {
                            isOK = false;
                            logger.error("RSU switch-participant-events returned an error: {}", response.body());
                        } else if (!rsuResponse.has("participants")) {
                            isOK = false;
                            logger.error("RSU switch-participant-events returned no participants for {} transfers: {}", transfers.size(), response.body());
                        } else {
                            JSONArray results = rsuResponse.getJSONArray("participants");
                            logger.trace(results.toString(4));

                            if (results.length() != transfers.size()) {
                                isOK = false;
                                logger.error("RSU switch-participant-events: sent {} transfers but got {} results back. Not updating registration ids.", transfers.size(), results.length());
                            } else {
                                for (int j = 0; j < results.length(); j++) {
                                    Integer newRegID = results.getJSONObject(j).getInt("registration_id");
                                    Registration r = transfers.get(j);
                                    Participant p = r.participant;
                                    if (!newRegID.equals(r.rsuRegID)) p.getRegID2RaceIDDMap().remove(r.rsuRegID);
                                    p.getRegID2RaceIDDMap().put(newRegID,r.newRSUEventID);
                                    logger.debug("RSU Switch Event: Old rsuID: {} New rsuID: {} New EventID: {} Participant: {}", r.rsuRegID, newRegID, r.newRSUEventID, p.fullNameProperty().getValueSafe());
                                    partDAO.updateParticipant(p);
                                }
                            }
                        }
                    } catch (Exception e){
                        isOK = false;
                        logger.error("Exception in RSU participant transfer: {} Response: {}",e.getMessage(), response.body(), e);
                    }

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
                    // Send each add as its own request so that the response (new
                    // registration id or error) maps unambiguously to one participant.
                    // RSU does not document per-item positions in batch error responses.
                    JSONArray eventAdds = updatesMap.get(event);
                    List<Registration> a = adds.get(event);
                    for (int idx = 0; idx < eventAdds.length(); idx++) {
                        Participant part = a.get(idx).participant;

                        StringBuilder rsuURL = new StringBuilder();
                        rsuURL.append("https://runsignup.com/Rest/race/").append(rsuConfig.rsuRaceID);
                        rsuURL.append("/participants?format=json");

                        // log it now before we tack on the key and secret
                        logger.debug("Add participant request for race_id={} and event_id={} for {}: {}", rsuConfig.rsuRaceID, event, part.fullNameProperty().getValueSafe(), rsuURL.toString());

                        JSONObject postData = new JSONObject();
                        postData.put("participants", new JSONArray().put(eventAdds.get(idx)));

                        StringBuilder postString = new StringBuilder();
                        postString.append("event_id=").append(event);
                        postString.append("&restrict_potential_dup=F&clear_null_corrals=F&request_format=json");
                        postString.append("&request=").append(postData.toString());

                        logger.debug("Add Participant request for race_id={} and event_id={}: {} \n {}", rsuConfig.rsuRaceID, event ,postString.toString());
                        HttpRequest request = authorizedRequest(rsuURL, currentAuth())
                                .header("Content-Type", "application/x-www-form-urlencoded")
                                .POST(HttpRequest.BodyPublishers.ofString(postString.toString()))
                                .build();
                        HttpClient client = HttpClient.newHttpClient();
                        try {
                            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                            if (response.statusCode() == 200) {
                                logger.debug("RSU add Participants Response: {}",response.body());
                                JSONObject rsuResponse = new JSONObject(response.body());

                                if (rsuResponse.has("participants") && rsuResponse.getJSONArray("participants").length() == 1) {
                                    Integer newRegID = rsuResponse.getJSONArray("participants").getJSONObject(0).getInt("registration_id");
                                    part.getRegID2RaceIDDMap().put(newRegID, event);
                                    partDAO.updateParticipant(part);
                                    logger.debug("RSU Add: {} added to RSU event {} with RegID {}", part.fullNameProperty().getValueSafe(), event, newRegID);
                                } else if (rsuResponse.has("error_details")) {
                                    // Look for a regID match (517) for this single participant
                                    JSONArray rsuError = rsuResponse.getJSONArray("error_details");
                                    boolean matched = false;
                                    for (int j = 0; j < rsuError.length(); j++) {
                                        JSONObject rsuErr = rsuError.getJSONObject(j);
                                        if (rsuErr.optInt("error_code") == 517 && rsuErr.optString("detailed_desc").matches("^\\d+$")) { // reg id match
                                            Integer matchRegID = Integer.valueOf(rsuErr.optString("detailed_desc"));
                                            logger.debug("RSU Add: {} matched existing RegID {} in event {}", part.fullNameProperty().getValueSafe(), matchRegID, event);
                                            part.getRegID2RaceIDDMap().put(matchRegID, event);
                                            partDAO.updateParticipant(part);
                                            matched = true;
                                        }
                                    }
                                    if (!matched) {
                                        logger.error("RSU Add: error adding {} to RSU event {}: {}", part.fullNameProperty().getValueSafe(), event, response.body());
                                        isOK = false;
                                    }
                                } else {
                                    logger.error("RSU Add: unexpected response adding {} to RSU event {}: {}", part.fullNameProperty().getValueSafe(), event, response.body());
                                    isOK = false;
                                }
                            } else {
                                logger.warn("RSU error Response of {} {}",response.statusCode(),response.body());
                                isOK = false;
                            }
                        } catch (Exception ex) {
                            logger.warn("Exception in swapRSUEvents adds: {}",ex.getMessage(),ex);
                            isOK = false;
                        }
                    }
                }
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

                JSONObject postData = new JSONObject();
                postData.put("participants", updatesMap.get(event));

                StringBuilder postString = new StringBuilder();
                postString.append("event_id=").append(event);
                postString.append("&restrict_potential_dup=T&clear_null_corrals=F&request_format=json");
                postString.append("&request=").append(postData.toString());


                logger.debug("Update Participant request for race_id={} and event_id={}: {} \n {}", rsuConfig.rsuRaceID, event ,postString.toString());
                HttpRequest request = authorizedRequest(rsuURL, currentAuth())
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
                        // RSU can return errors with a 200
                        try {
                            JSONObject rsuResponse = new JSONObject(response.body());
                            if (rsuResponse.has("error") || rsuResponse.has("error_details")) {
                                logger.warn("RSU updateParticipants returned an error for event {}: {}", event, response.body());
                                noIssues.set(false);
                            }
                        } catch (Exception jex) {
                            logger.warn("RSU updateParticipants: unable to parse the response for event {}: {}", event, response.body());
                            noIssues.set(false);
                        }
                    } else {
                        logger.warn("RSU error Response of {} {}",response.statusCode(),response.body());
                        noIssues.set(false);
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
            logger.debug("RSU Wizard: login type {} pre-filled from the event DB", rsuConf.rsuLoginType);
        } else {
            setupData.put("rsuLoginType", pikaPrefs.get("rsuLoginType", ""));
            logger.debug("RSU Wizard: login type '{}' pre-filled from the global preferences", setupData.get("rsuLoginType"));
        }

        // RSU OAuth tokens (event DB only) and username (event DB, then global prefs)
        if (RSUConfig.LOGIN_OAUTH.equals(rsuConf.rsuLoginType) && rsuConf.rsuRefreshToken != null && !rsuConf.rsuRefreshToken.isBlank()) {
            setupData.put("rsuAccessToken", rsuConf.rsuAccessToken == null ? "" : rsuConf.rsuAccessToken);
            setupData.put("rsuRefreshToken", rsuConf.rsuRefreshToken);
            setupData.put("rsuTokenExpires", rsuConf.rsuTokenExpires.toString());
            logger.debug("RSU Wizard: existing OAuth tokens found in the event DB (access={}, refresh={}, expires={})",
                    RSUOAuthClient.mask(rsuConf.rsuAccessToken), RSUOAuthClient.mask(rsuConf.rsuRefreshToken), Instant.ofEpochSecond(rsuConf.rsuTokenExpires));
        } else {
            logger.debug("RSU Wizard: no existing OAuth tokens in the event DB");
        }
        if (rsuConf.rsuOAuthUser != null && !rsuConf.rsuOAuthUser.isBlank()) {
            setupData.put("rsuOAuthUser", rsuConf.rsuOAuthUser);
        } else {
            setupData.put("rsuOAuthUser", pikaPrefs.get("rsuOAuthUser", ""));
        }
        logger.debug("RSU Wizard: default OAuth user '{}'", setupData.get("rsuOAuthUser"));

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

        final String loginV1Label = "API Key/Secret (v1)";
        final String loginV2Label = "API Key/Secret (v2)";
        final String loginOAuthLabel = "RunSignUp Login (OAuth)";

        rsuLoginGrid.add(new Label("Login Method:"), 0, row);
        ComboBox<String> rsuLoginTypeComboBox = new ComboBox<>();
        rsuLoginTypeComboBox.getItems().addAll(loginV1Label, loginV2Label, loginOAuthLabel);
        if (RSUConfig.LOGIN_API_V1.equals(setupData.get("rsuLoginType")) || RSUConfig.LOGIN_PASSWORD.equals(setupData.get("rsuLoginType"))) {
            rsuLoginTypeComboBox.getSelectionModel().select(loginV1Label);
        } else if (RSUConfig.LOGIN_OAUTH.equals(setupData.get("rsuLoginType"))) {
            rsuLoginTypeComboBox.getSelectionModel().select(loginOAuthLabel);
        } else {
            rsuLoginTypeComboBox.getSelectionModel().select(loginV2Label);
        }
        logger.debug("RSU Wizard: initial login method selection: {}", rsuLoginTypeComboBox.getSelectionModel().getSelectedItem());
        GridPane.setHgrow(rsuLoginTypeComboBox, Priority.ALWAYS);
        rsuLoginGrid.add(rsuLoginTypeComboBox, 1, row++);

        Label rsuKeyLabel = new Label("Key:");
        rsuLoginGrid.add(rsuKeyLabel, 0, row);
        TextField rsuUsernameTextField = createTextField("rsuKey");
        rsuUsernameTextField.setText(setupData.get("rsuKey"));
        GridPane.setHgrow(rsuUsernameTextField, Priority.ALWAYS);
        rsuLoginGrid.add(rsuUsernameTextField, 1, row++);

        Label rsuSecretLabel = new Label("Secret:");
        rsuLoginGrid.add(rsuSecretLabel, 0, row);
        PasswordField rsuPasswordTextField = new PasswordField();
        GridPane.setHgrow(rsuPasswordTextField, Priority.ALWAYS);
        rsuPasswordTextField.setText(setupData.get("rsuSecret"));
        GridPane.setHgrow(rsuPasswordTextField, Priority.ALWAYS);
        rsuLoginGrid.add(rsuPasswordTextField, 1, row++);

        // OAuth: Login button + current account
        Label rsuAccountLabel = new Label("Account:");
        rsuLoginGrid.add(rsuAccountLabel, 0, row);
        Label oauthUserLabel = new Label();
        if (setupData.containsKey("rsuRefreshToken")) {
            oauthUserLabel.setText("Logged in as: " + (setupData.get("rsuOAuthUser").isBlank() ? "(unknown)" : setupData.get("rsuOAuthUser")));
        } else if (!setupData.get("rsuOAuthUser").isBlank()) {
            oauthUserLabel.setText("Not logged in (last used: " + setupData.get("rsuOAuthUser") + ")");
        } else {
            oauthUserLabel.setText("Not logged in");
        }
        Button oauthLoginButton = new Button("Login with RunSignUp");
        if (!RSUOAuthClient.isConfigured()) {
            oauthLoginButton.setDisable(true);
            oauthUserLabel.setText("OAuth login is not available in this build");
        }
        Pane oauthSpring = new Pane();
        HBox.setHgrow(oauthSpring, Priority.ALWAYS);
        HBox oauthHBox = new HBox(oauthUserLabel, oauthSpring, oauthLoginButton);
        oauthHBox.setSpacing(4);
        oauthHBox.setAlignment(Pos.CENTER_LEFT);
        GridPane.setHgrow(oauthHBox, Priority.ALWAYS);
        rsuLoginGrid.add(oauthHBox, 1, row++);

        rsuLoginGrid.add(new Label("Status:"), 0, row);
        Button validateButton = new Button("Validate");
        Label loginSuccessLabel = new Label("Unchecked");
        Pane loginSpring = new Pane();
        HBox.setHgrow(loginSpring, Priority.ALWAYS);
        HBox validateHBox = new HBox(loginSuccessLabel, loginSpring, validateButton);
        validateHBox.setSpacing(4);
        validateHBox.setAlignment(Pos.CENTER_LEFT);
        rsuLoginGrid.add(validateHBox, 1, row++);

        // Show the key/secret fields or the OAuth login based on the login method
        Runnable updateLoginFields = () -> {
            boolean oauth = loginOAuthLabel.equals(rsuLoginTypeComboBox.getSelectionModel().getSelectedItem());
            for (javafx.scene.Node n : List.of(rsuKeyLabel, rsuUsernameTextField, rsuSecretLabel, rsuPasswordTextField)) {
                n.setVisible(!oauth);
                n.setManaged(!oauth);
            }
            rsuAccountLabel.setVisible(oauth);
            rsuAccountLabel.setManaged(oauth);
            oauthHBox.setVisible(oauth);
            oauthHBox.setManaged(oauth);
        };
        updateLoginFields.run();

        rsuLoginTypeComboBox.getSelectionModel().selectedItemProperty().addListener((observable, oldValue, newValue) -> {
            logger.debug("RSU Wizard: login method changed from {} to {}", oldValue, newValue);
            pane1OkayToGo.setValue(false);
            loginSuccessLabel.setText("Unchecked");
            updateLoginFields.run();
        });

        // If the username or password fields change, force a re-validation
        rsuUsernameTextField.textProperty().addListener((observable, oldValue, newValue) -> {
            pane1OkayToGo.setValue(false);
        });
        rsuPasswordTextField.textProperty().addListener((observable, oldValue, newValue) -> {
            pane1OkayToGo.setValue(false);
        });

        oauthLoginButton.setOnAction((e) -> {
            logger.debug("RSU Wizard: starting OAuth browser login");
            pane1OkayToGo.setValue(false);
            oauthLoginButton.setDisable(true);
            validateButton.setDisable(true);
            loginSuccessLabel.setText("Waiting for the RunSignUp login in your browser...");

            Task<Map<String,String>> oauthTask = new Task<>() {
                @Override
                protected Map<String,String> call() throws Exception {
                    RSUOAuthClient.TokenResult tokens = RSUOAuthClient.login();
                    Map<String,String> result = new HashMap<>();
                    result.put("rsuAccessToken", tokens.accessToken());
                    result.put("rsuRefreshToken", tokens.refreshToken() == null ? "" : tokens.refreshToken());
                    result.put("rsuTokenExpires", Long.toString(tokens.expiresAtEpoch()));
                    result.put("rsuOAuthUser", RSUOAuthClient.lookupUser(tokens.accessToken()));
                    String check = checkRSULogin(new RSUAuth(RSUConfig.LOGIN_OAUTH, null, null, tokens.accessToken(), null, null), setupData.get("eventDate"));
                    if (check != null) result.put("error", check);
                    return result;
                }
            };
            oauthTask.setOnSucceeded(ev -> {
                Map<String,String> result = oauthTask.getValue();
                oauthLoginButton.setDisable(false);
                validateButton.setDisable(false);
                setupData.put("rsuAccessToken", result.get("rsuAccessToken"));
                setupData.put("rsuRefreshToken", result.get("rsuRefreshToken"));
                setupData.put("rsuTokenExpires", result.get("rsuTokenExpires"));
                if (!result.get("rsuOAuthUser").isBlank()) setupData.put("rsuOAuthUser", result.get("rsuOAuthUser"));
                oauthUserLabel.setText("Logged in as: " + (setupData.get("rsuOAuthUser").isBlank() ? "(unknown)" : setupData.get("rsuOAuthUser")));
                if (result.containsKey("error")) {
                    logger.debug("RSU Wizard: OAuth login succeeded but validation failed: {}", result.get("error"));
                    loginSuccessLabel.setText(result.get("error"));
                } else {
                    logger.debug("RSU Wizard: OAuth login succeeded for user '{}'", setupData.get("rsuOAuthUser"));
                    setupData.put("rsuLoginType", RSUConfig.LOGIN_OAUTH);
                    loginSuccessLabel.setText("Valid");
                    pane1OkayToGo.setValue(true);
                }
            });
            oauthTask.setOnFailed(ev -> {
                Throwable t = oauthTask.getException();
                logger.debug("RSU Wizard: OAuth login failed: {}", t == null ? "unknown" : t.getMessage(), t);
                oauthLoginButton.setDisable(false);
                validateButton.setDisable(false);
                loginSuccessLabel.setText("Login failed: " + (t == null ? "unknown error" : t.getMessage()));
            });
            Thread oauthThread = new Thread(oauthTask);
            oauthThread.setDaemon(true);
            oauthThread.start();
        });

        validateButton.setOnAction((e) -> {
            String selected = rsuLoginTypeComboBox.getSelectionModel().getSelectedItem();
            logger.debug("RSU Wizard: validating login method {}", selected);
            pane1OkayToGo.setValue(false);

            if (loginOAuthLabel.equals(selected)) {
                // Validate using the existing tokens (refreshing them if needed)
                if (!setupData.containsKey("rsuRefreshToken") || setupData.get("rsuRefreshToken").isBlank()) {
                    logger.debug("RSU Wizard: no OAuth tokens to validate, login required");
                    loginSuccessLabel.setText("Click 'Login with RunSignUp' to log in");
                    return;
                }
                long expires = Long.parseLong(setupData.getOrDefault("rsuTokenExpires", "0"));
                long remaining = expires - Instant.now().getEpochSecond();
                logger.debug("RSU Wizard: existing OAuth access token expires in {}s", remaining);
                if (remaining < 86400 || setupData.getOrDefault("rsuAccessToken", "").isBlank()) {
                    try {
                        logger.debug("RSU Wizard: refreshing OAuth access token");
                        RSUOAuthClient.TokenResult t = RSUOAuthClient.refresh(setupData.get("rsuRefreshToken"));
                        setupData.put("rsuAccessToken", t.accessToken());
                        setupData.put("rsuRefreshToken", t.refreshToken());
                        setupData.put("rsuTokenExpires", Long.toString(t.expiresAtEpoch()));
                    } catch (Exception ex) {
                        logger.debug("RSU Wizard: OAuth refresh failed: {}", ex.getMessage());
                        setupData.remove("rsuRefreshToken");
                        setupData.remove("rsuAccessToken");
                        oauthUserLabel.setText("Not logged in");
                        loginSuccessLabel.setText("Login expired. Click 'Login with RunSignUp'");
                        return;
                    }
                }
                String check = checkRSULogin(new RSUAuth(RSUConfig.LOGIN_OAUTH, null, null, setupData.get("rsuAccessToken"), null, null), setupData.get("eventDate"));
                if (check == null) {
                    setupData.put("rsuLoginType", RSUConfig.LOGIN_OAUTH);
                    loginSuccessLabel.setText("Valid");
                    pane1OkayToGo.setValue(true);
                } else {
                    loginSuccessLabel.setText(check);
                }
                logger.debug("RSU Wizard: OAuth validation result: {}", check == null ? "Valid" : check);
            } else {
                // There is no login equivalent for the key/secret
                // so we will just make a call to /Rest/races/ and
                // see how many we get back
                String type = loginV1Label.equals(selected) ? RSUConfig.LOGIN_API_V1 : RSUConfig.LOGIN_API_V2;
                RSUAuth auth = new RSUAuth(type, rsuUsernameTextField.getText(), rsuPasswordTextField.getText(), null, null, null);
                String check = checkRSULogin(auth, setupData.get("eventDate"));
                if (check == null) {
                    pane1OkayToGo.setValue(true);
                    loginSuccessLabel.setText("Valid");
                    setupData.put("rsuLoginType", type);
                    setupData.put("rsuKey", rsuUsernameTextField.getText());
                    setupData.put("rsuSecret", rsuPasswordTextField.getText());
                } else {
                    loginSuccessLabel.setText(check);
                }
                logger.debug("RSU Wizard: {} validation result: {}", type, check == null ? "Valid" : check);
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
                requestURL.append("?format=json").append("&include_event_days=T");
                if (isAPIKeyLogin(setupData.get("rsuLoginType"))) requestURL.append("&only_partner_races=T");
                requestURL.append("&page=1&results_per_page=50&sort=name+ASC");
                requestURL.append("&start_date=").append(setupData.get("eventDate"));
                requestURL.append("&end_date=").append(setupData.get("eventDate"));

                logger.debug("RSU Get Races Request URL: {}", requestURL.toString());

                HttpRequest request = authorizedRequest(requestURL, wizardAuth(setupData))
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
                requestURL.append("?format=json").append("&future_events_only=F&most_recent_events_only=F");
                requestURL.append("&race_event_days_id=").append(setupData.get("race_event_days_id"));
                requestURL.append("&include_questions=T");

                logger.debug("RSU Get Race Request URL: {}", requestURL.toString());

                HttpRequest request = authorizedRequest(requestURL, wizardAuth(setupData))
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

                if (logger.isDebugEnabled()) {
                    Map<String,String> masked = new HashMap<>(setupData);
                    for (String k : List.of("rsuSecret", "rsuAccessToken", "rsuRefreshToken", "rsuTempKey", "rsuTempSecret")) {
                        if (masked.containsKey(k)) masked.put(k, RSUOAuthClient.mask(masked.get(k)));
                    }
                    logger.debug("setupData: {} ", masked);
                }

                String loginType = setupData.get("rsuLoginType");
                rsuConf.rsuLoginType = loginType;

                if (RSUConfig.LOGIN_OAUTH.equals(loginType)) {
                    // Save the login type and RSU username to the global prefs (tokens stay in the event DB)
                    pikaPrefs.set("rsuLoginType", loginType);
                    pikaPrefs.set("rsuOAuthUser", setupData.getOrDefault("rsuOAuthUser", ""));
                    logger.debug("RSU Wizard: saved global prefs rsuLoginType={} rsuOAuthUser={}", loginType, setupData.get("rsuOAuthUser"));

                    // Save the tokens to the event db
                    rsuConf.rsuAccessToken = setupData.get("rsuAccessToken");
                    rsuConf.rsuRefreshToken = setupData.get("rsuRefreshToken");
                    rsuConf.rsuTokenExpires = Long.valueOf(setupData.getOrDefault("rsuTokenExpires", "0"));
                    rsuConf.rsuOAuthUser = setupData.get("rsuOAuthUser");
                    logger.debug("RSU Wizard: saving OAuth tokens to event DB (access={}, refresh={}, expires={}, user={})",
                            RSUOAuthClient.mask(rsuConf.rsuAccessToken), RSUOAuthClient.mask(rsuConf.rsuRefreshToken),
                            Instant.ofEpochSecond(rsuConf.rsuTokenExpires), rsuConf.rsuOAuthUser);
                } else {
                    // Save the rsu Key/Secret to the global prefs
                    pikaPrefs.set("rsuLoginType", loginType);
                    pikaPrefs.set("rsuKey", setupData.get("rsuKey"));
                    pikaPrefs.setObfuscated("rsuSecret", setupData.get("rsuSecret"));
                    logger.debug("RSU Wizard: saved global prefs rsuLoginType={} rsuKey={} rsuSecret={}",
                            loginType, setupData.get("rsuKey"), RSUOAuthClient.mask(setupData.get("rsuSecret")));

                    // Save the Key/Secret to the race db
                    rsuConf.rsuSecret = setupData.get("rsuSecret");
                    rsuConf.rsuKey = setupData.get("rsuKey");

                    // clear out any old OAuth tokens
                    rsuConf.rsuAccessToken = null;
                    rsuConf.rsuRefreshToken = null;
                    rsuConf.rsuTokenExpires = 0L;
                    logger.debug("RSU Wizard: saving {} key {} to event DB", loginType, setupData.get("rsuKey"));
                }

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

    ////////////////////////////////////////////////////////////////////////
    //
    // Authentication helpers
    //
    ////////////////////////////////////////////////////////////////////////

    // Credentials for a single RSU request
    record RSUAuth(String type, String key, String secret, String accessToken, String tempKey, String tempSecret) { }

    private long lastForcedRefresh = 0L;
    private boolean reauthAlertShown = false;

    private static boolean isAPIKeyLogin(String type) {
        return RSUConfig.LOGIN_API_V1.equals(type) || RSUConfig.LOGIN_API_V2.equals(type);
    }

    /**
     * Credentials from the saved RSUConfig. For OAuth, refreshes the
     * access token if needed. For the legacy username/password, refreshes
     * the tmp_key/tmp_secret.
     */
    private RSUAuth currentAuth() {
        String type = rsuConfig.rsuLoginType;
        if (RSUConfig.LOGIN_OAUTH.equals(type)) {
            ensureFreshToken(false);
        } else if (RSUConfig.LOGIN_PASSWORD.equals(type)) {
            updateRSUKeys();
        }
        return new RSUAuth(type, rsuConfig.rsuKey, rsuConfig.rsuSecret, rsuConfig.rsuAccessToken, rsuConfig.rsuTempKey, rsuConfig.rsuTempSecret);
    }

    /**
     * Credentials from the in-progress setup wizard data
     */
    private RSUAuth wizardAuth(Map<String,String> setupData) {
        return new RSUAuth(setupData.get("rsuLoginType"), setupData.get("rsuKey"), setupData.get("rsuSecret"),
                setupData.get("rsuAccessToken"), setupData.get("rsuTempKey"), setupData.get("rsuTempSecret"));
    }

    /**
     * Appends the auth parameters to the url and returns a request builder
     * with any required auth headers already set.
     */
    private HttpRequest.Builder authorizedRequest(StringBuilder url, RSUAuth auth) {
        HttpRequest.Builder builder = HttpRequest.newBuilder();
        String type = auth.type() == null ? RSUConfig.LOGIN_API_V1 : auth.type();
        String logURL = url.toString();
        char sep = url.indexOf("?") >= 0 ? '&' : '?';

        switch (type) {
            case RSUConfig.LOGIN_API_V2 -> {
                url.append(sep).append("rsu_api_key=").append(URLEncoder.encode(nz(auth.key()), StandardCharsets.UTF_8));
                builder.header("X-RSU-API-SECRET", nz(auth.secret()));
                logURL += sep + "rsu_api_key=" + nz(auth.key());
                logger.debug("RSU request auth: v2 API key, headers set: X-RSU-API-SECRET={}", RSUOAuthClient.mask(auth.secret()));
            }
            case RSUConfig.LOGIN_OAUTH -> {
                builder.header("Authorization", "Bearer " + nz(auth.accessToken()));
                logger.debug("RSU request auth: OAuth, headers set: Authorization=Bearer {}", RSUOAuthClient.mask(auth.accessToken()));
            }
            case RSUConfig.LOGIN_PASSWORD -> {
                url.append(sep).append("tmp_key=").append(nz(auth.tempKey()));
                url.append("&tmp_secret=").append(nz(auth.tempSecret()));
                logURL += sep + "tmp_key=" + RSUOAuthClient.mask(auth.tempKey()) + "&tmp_secret=" + RSUOAuthClient.mask(auth.tempSecret());
                logger.debug("RSU request auth: legacy username/password tmp_key/tmp_secret");
            }
            default -> {
                url.append(sep).append("api_key=").append(URLEncoder.encode(nz(auth.key()), StandardCharsets.UTF_8));
                url.append("&api_secret=").append(URLEncoder.encode(nz(auth.secret()), StandardCharsets.UTF_8));
                logURL += sep + "api_key=" + nz(auth.key()) + "&api_secret=" + RSUOAuthClient.mask(auth.secret());
                logger.debug("RSU request auth: v1 API key/secret");
            }
        }
        logger.debug("RSU request URL: {}", logURL);

        return builder.uri(URI.create(url.toString()));
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }

    /**
     * Make sure the OAuth access token is valid. Refreshes it if it expires
     * within the next 24 hours (or if forced) and saves the new tokens.
     * Returns false if we no longer have a usable token.
     */
    private synchronized boolean ensureFreshToken(boolean force) {
        long now = Instant.now().getEpochSecond();
        long remaining = rsuConfig.rsuTokenExpires - now;
        boolean haveToken = rsuConfig.rsuAccessToken != null && !rsuConfig.rsuAccessToken.isBlank();
        logger.debug("RSU OAuth: access token expires at {} ({}s remaining), force={}",
                Instant.ofEpochSecond(rsuConfig.rsuTokenExpires), remaining, force);

        if (!force && haveToken && remaining > 86400) return true;

        if (force && now - lastForcedRefresh < 60) {
            logger.debug("RSU OAuth: forced refresh skipped, last forced refresh was {}s ago", now - lastForcedRefresh);
            return haveToken;
        }
        if (force) lastForcedRefresh = now;

        logger.debug("RSU OAuth: refreshing access token (force={})", force);
        try {
            RSUOAuthClient.TokenResult t = RSUOAuthClient.refresh(rsuConfig.rsuRefreshToken);
            rsuConfig.rsuAccessToken = t.accessToken();
            rsuConfig.rsuRefreshToken = t.refreshToken();
            rsuConfig.rsuTokenExpires = t.expiresAtEpoch();
            reauthAlertShown = false;
            logger.debug("RSU OAuth: refresh succeeded, new expiration {}", Instant.ofEpochSecond(t.expiresAtEpoch()));
            if (rsuConfig.getID() != null) saveRSUConfig();
            return true;
        } catch (Exception ex) {
            logger.error("RSU OAuth: unable to refresh the access token: {}", ex.getMessage());
            if (!force && haveToken && remaining > 0) {
                logger.debug("RSU OAuth: refresh failed but the current access token is still valid for {}s", remaining);
                return true;
            }
            if (!reauthAlertShown) {
                reauthAlertShown = true;
                Platform.runLater(() -> {
                    Alert alert = new Alert(AlertType.WARNING);
                    alert.setTitle("RunSignUp Login Required");
                    alert.setHeaderText("Your RunSignUp login has expired");
                    alert.setContentText("PikaTimer was unable to renew the RunSignUp login. Please re-run the RunSignUp setup wizard to log in again.");
                    alert.show();
                });
            }
            return false;
        }
    }

    /**
     * Called when RSU rejects a request. Renew the credentials if we can.
     */
    private void handleAuthFailure() {
        logger.debug("RSU request failed, checking credentials for login type {}", rsuConfig.rsuLoginType);
        if (RSUConfig.LOGIN_OAUTH.equals(rsuConfig.rsuLoginType)) {
            ensureFreshToken(true);
        } else if (RSUConfig.LOGIN_PASSWORD.equals(rsuConfig.rsuLoginType)) {
            updateRSUKeys();
        }
    }

    private synchronized void saveRSUConfig() {
        logger.debug("RunSignUpDAO: saving RSU config");
        Session s = HibernateUtil.getSessionFactory().getCurrentSession();
        boolean startedTx = false;
        if (!s.getTransaction().isActive()) {
            s.beginTransaction();
            startedTx = true;
        }
        s.saveOrUpdate(rsuConfig);
        if (startedTx) s.getTransaction().commit();
    }

    /**
     * Validate the credentials by asking RSU for the races on the event date.
     * Returns null if valid, otherwise a message for the user.
     */
    private String checkRSULogin(RSUAuth auth, String eventDate) {
        StringBuilder requestURL = new StringBuilder();
        requestURL.append("https://runsignup.com/Rest/races");
        requestURL.append("?format=json").append("&include_event_days=T");
        if (isAPIKeyLogin(auth.type())) requestURL.append("&only_partner_races=T");
        requestURL.append("&page=1&results_per_page=50&sort=name+ASC");
        requestURL.append("&start_date=").append(eventDate);
        requestURL.append("&end_date=").append(eventDate);

        HttpRequest request = authorizedRequest(requestURL, auth).build();
        try {
            HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
            logger.debug("RSU login check response status {}: {}", response.statusCode(), response.body());
            if (response.statusCode() != 200) {
                logger.error("Error in RSU Login: {} ", response.body());
                return (response.statusCode() == 401 || response.statusCode() == 403) ? "Invalid credentials" : "Error in connecting to RunSignUp";
            }
            JSONObject rsuResponse = new JSONObject(response.body());
            if (!rsuResponse.has("races")) {
                logger.error("Error in RSU Login: {} ", response.body());
                return isAPIKeyLogin(auth.type()) ? "Invalid API Secret / Key" : "RunSignUp login was rejected";
            }
            int responseSize = rsuResponse.getJSONArray("races").length();
            logger.debug("RSU login check found {} races for {}", responseSize, eventDate);
            if (responseSize == 0) return "Valid login, but no races found for " + eventDate;
            return null;
        } catch (Exception ex) {
            logger.error("Exception in HttpClient response: ", ex);
            return "Error in RSU Login Request";
        }
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
    
    ////////////////////////////////////////////////////////////////////////
    //
    // Sync helpers
    //
    ////////////////////////////////////////////////////////////////////////

    /**
     * Is the Pika race linked to an RSU event?
     */
    public boolean isRaceMappedToRSU(Race r) {
        if (r == null || !isSetup().get()) return false;
        RSUConfig c = getRSUConfig();
        return c.raceToEventMap != null && c.raceToEventMap.get(r.getID()) != null && !c.raceToEventMap.get(r.getID()).equals(-1);
    }

    /**
     * RSU registrations can not be removed via the API. A change to a
     * participant's waves is only allowed if every RSU registration can either
     * stay in its race or be transferred to another race that is linked to an
     * RSU event.
     */
    public boolean allowsWaveChange(Participant p, Collection<Wave> newWaves) {
        if (p == null || !isSetup().get() || p.getRegID2RaceIDDMap().isEmpty()) return true;
        RSUConfig c = getRSUConfig();

        // Pika races that the participant currently has RSU registrations for
        Set<Integer> registeredRaces = new HashSet<>();
        p.getRegID2RaceIDDMap().values().forEach(ev -> {
            Integer raceID = c.eventToRaceMap == null ? null : c.eventToRaceMap.get(ev);
            if (raceID != null && !raceID.equals(-1)) registeredRaces.add(raceID);
        });

        // Pika races in the new set of waves that are linked to an RSU event
        Set<Integer> newMappedRaces = new HashSet<>();
        newWaves.forEach(w -> { if (w != null && isRaceMappedToRSU(w.getRace())) newMappedRaces.add(w.getRace().getID()); });

        boolean ok = newMappedRaces.size() >= registeredRaces.size();
        logger.debug("allowsWaveChange: {} has RSU registrations for races {}, new RSU linked races {}: {}",
                p.fullNameProperty().getValueSafe(), registeredRaces, newMappedRaces, ok ? "allowed" : "blocked");
        return ok;
    }

    /**
     * Does the participant have an RSU registration (other than excludeRegID)
     * that maps to the given Pika race?
     */
    private boolean otherRegInRace(Participant p, Integer excludeRegID, Integer pikaRaceID) {
        for (Map.Entry<Integer,Integer> e : p.getRegID2RaceIDDMap().entrySet()) {
            if (Objects.equals(e.getKey(), excludeRegID)) continue;
            if (Objects.equals(rsuConfig.eventToRaceMap.get(e.getValue()), pikaRaceID)) return true;
        }
        return false;
    }

    /**
     * The wave in the given race for this bib, or the default wave if the
     * bib does not map to a wave in this race.
     */
    private Wave waveForRace(Race race, String bib, Wave defaultWave) {
        if (race.getWaves().size() <= 1 || bib == null || bib.isBlank()) return defaultWave;
        for (Wave w : partDAO.getWaveByBib(bib)) {
            if (Objects.equals(w.getRace().getID(), race.getID())) return w;
        }
        logger.trace("waveForRace: bib {} does not map to a wave in {}, using the default wave {}", bib, race.getRaceName(), defaultWave.getWaveName());
        return defaultWave;
    }

    record Registration(Integer rsuRegID, Integer newRSUEventID, Integer newPikaRaceID, Participant participant) { };

}
