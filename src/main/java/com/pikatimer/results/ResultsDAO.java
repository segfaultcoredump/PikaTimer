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
package com.pikatimer.results;

import com.pikatimer.participant.Participant;
import com.pikatimer.participant.ParticipantDAO;
import com.pikatimer.participant.Status;
import com.pikatimer.race.CourseRecord;
import com.pikatimer.race.Race;
import com.pikatimer.race.RaceDAO;
import com.pikatimer.race.Wave;
import com.pikatimer.timing.CookedTimeData;
import com.pikatimer.timing.Split;
import com.pikatimer.timing.TimeOverride;
import com.pikatimer.timing.TimeOverrideType;
import com.pikatimer.timing.TimingDAO;
import com.pikatimer.util.DurationFormatter;
import com.pikatimer.util.HTTPServices;
import com.pikatimer.util.HibernateUtil;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import java.util.stream.Collectors;
import javafx.application.Platform;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import org.apache.commons.lang3.ObjectUtils;
import org.hibernate.Session;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jcgarner
 */
public class ResultsDAO {
    private static final Logger logger = LoggerFactory.getLogger(ResultsDAO.class);
    
    private static final BlockingQueue<String> resultsQueue = new ArrayBlockingQueue(100000);
    private static final BlockingQueue<Result> checkForCRQueue = new ArrayBlockingQueue(100000);
    private Map<Integer,ObservableList<Result>> raceResultsMap;
    private static final Map<String,Map<Integer,Result>> resultsMap = new ConcurrentHashMap<>(); 
    private static final TimingDAO timingDAO = TimingDAO.getInstance();
    private static final ParticipantDAO participantDAO = ParticipantDAO.getInstance();
    private static final RaceDAO raceDAO = RaceDAO.getInstance();
    private static final ObservableList<ReportDestination> reportDestinationList = FXCollections.observableArrayList(ReportDestination.extractor());
    
    private static final BooleanProperty reportDestinationListInitialized = new SimpleBooleanProperty(FALSE);
        
    /**
    * SingletonHolder is loaded on the first execution of Singleton.getInstance() 
    * or the first access to SingletonHolder.INSTANCE, not before.
    */
    private static class SingletonHolder { 
            private static final ResultsDAO INSTANCE = new ResultsDAO();
    }

    public static ResultsDAO getInstance() {
            return SingletonHolder.INSTANCE;
    }
    
    public BlockingQueue<String> getResultsQueue(){
        return resultsQueue;
    }
    
    public ObservableList<Result> getResults(Integer raceID) {
        
        // if the results list is null, then let's create one,
        // fetch fetch existing datapoints from the DB,
        // and add them to the resultsList;
        
        if (raceResultsMap == null) {
            raceResultsMap = new ConcurrentHashMap();
            
            if(!raceResultsMap.containsKey(raceID)) raceResultsMap.put(raceID, FXCollections.observableArrayList(Result.extractor()));

            Task processNewResult = new Task<Void>() {

                @Override 
                public Void call() {
                   final List<Result> results = new ArrayList(); 

                   Session s=HibernateUtil.getSessionFactory().getCurrentSession();
                   s.beginTransaction();
                   logger.debug("ResultsDAO: Runing the getResults querry");

                    try {  
                        results.addAll(s.createQuery("from Result").list());
                    } catch (Exception e) {
                        logger.debug(e.getMessage());
                    } 
                    s.getTransaction().commit();  
                    
                    final CountDownLatch doneLatch = new CountDownLatch(results.size());
                    results.stream().forEach(r -> {
                        //resultsMap.put(r.getBib() + " " + r.getRaceID(), r);
                        
                        //We can do this outside of the UI thread since the UI 
                        // does not know about it yet... 
                        r.recalcTimeProperties();
                        
                        if (!resultsMap.containsKey(r.getBib())) resultsMap.put(r.getBib(), new HashMap());
                        resultsMap.get(r.getBib()).put(r.getRaceID(),r);
                        
                        if(!raceResultsMap.containsKey(r.getRaceID())) raceResultsMap.put(r.getRaceID(), FXCollections.observableArrayList(Result.extractor()));
                        
                        
                        Platform.runLater(() -> {
                            //raceResultsMap.get(r.getRaceID()).remove(r);
                            if(!r.isEmpty()){
                                raceResultsMap.get(r.getRaceID()).add(r);
                                logger.trace("ResultsDAO read from DB added " + r.getBib() + " from race " + r.getRaceID() + " new total " + raceResultsMap.get(r.getRaceID()).size() );
                            }
                            doneLatch.countDown();
                        });

                    });
                    logger.debug("Waiting for raceResultsMap to get updated...");
                    try {
                        doneLatch.await();
                        logger.debug("Done waiting for raceResultsMap!");
                    } catch (InterruptedException e) {
                        // ignore exception
                    }
                    reprocessAllCRs();

                    logger.debug("ResultsDAO: new result processing thread started");
                    while(true) {
                        Set<String> pendingBibs = new HashSet();
                        try {
                            
                            logger.debug("ResultsDAO ProcessNewResult Thread: Waiting for more bibs to process...");
                            pendingBibs.add(resultsQueue.take());
                            logger.debug("ResultsDAO ProcessNewResult Thread: The wait is over...");
                            Thread.sleep(10); // Times rarely come in 1 at a time

                            //resultsQueue.drainTo(pendingBibs,299);  // 500 total
                            resultsQueue.drainTo(pendingBibs);
                            logger.debug("ResultsDAO ProcessNewResult Thread: Processing: " + pendingBibs.size());

                            List<Result> pendingResults = new ArrayList();
                            try {
                                pendingBibs.stream().forEach(pb -> {
                                    processBib(pb);
                                    if (!resultsMap.get(pb).keySet().isEmpty()) {
                                        logger.trace("ResultsDAO ProcessNewResult Thread: resultsMap has " + resultsMap.get(pb).size() + " entries");
                                        pendingResults.addAll(resultsMap.get(pb).values());
                                    }
                                    
                                });
                            } catch (Exception e) {
                                e.printStackTrace();
                            } 
                            
                            try {
                                s = HibernateUtil.getSessionFactory().getCurrentSession();
                                s.beginTransaction();
                                int count = 0;
                                Iterator<Result> addIterator = pendingResults.iterator();
                                while (addIterator.hasNext()) {
                                    Result c = addIterator.next();
                                    if (c.isEmpty() && c.getID() != null) {    
                                        logger.trace("Deleting " + c.getBib());
                                        s.delete(c);    
                                        resultsMap.get(c.getBib()).remove(c.getRaceID());
                                    } else {                    
                                        logger.trace("Saving " + c.getBib());
                                        s.saveOrUpdate(c);                                        
                                    }
                                    if (++count % 20 == 0) {
                                        //flush a batch of updates and release memory:
                                        s.flush();
                                        s.clear();
                                    }
                                }
                                s.getTransaction().commit();                                
                            } catch (Exception e) {
                                e.printStackTrace();
                            } 
                            
                            pendingResults.stream().forEach(r -> {
                                if(!raceResultsMap.containsKey(r.getRaceID())) raceResultsMap.put(r.getRaceID(), FXCollections.observableArrayList(Result.extractor()));
                            });
                            Platform.runLater(() -> {
                                pendingResults.stream().forEach(r -> {
                                    //This causes the AAIOB error due to a java bug until we fix the extractor and the tableview to only display properties
                                    //if (r.isEmpty() && raceResultsMap.get(r.getRaceID()).contains(r)) raceResultsMap.get(r.getRaceID()).remove(r);
                                    
                                    if(!r.isEmpty()){
                                        if (!raceResultsMap.get(r.getRaceID()).contains(r)) {
                                            r.recalcTimeProperties();
                                            raceResultsMap.get(r.getRaceID()).add(r);
                                            logger.trace("ResultsDAO new/updated result added " + r.getBib() + " from race " + r.getRaceID() + " new total " + raceResultsMap.get(r.getRaceID()).size() );
                                        } else r.recalcTimeProperties();
                                    } else if (raceResultsMap.get(r.getRaceID()).contains(r)){
                                        raceResultsMap.get(r.getRaceID()).remove(r);
                                    }
                                });

                            });
                            pendingResults.stream().forEach(r -> {
                                if (!r.isEmpty() && r.getFinish()>0) {
                                    String raceName = "";
                                    Race race = RaceDAO.getInstance().getRaceByID(r.getRaceID());
                                    
                                    if (RaceDAO.getInstance().listRaces().size() > 1) raceName = race.getRaceName();
                                        
                                    String bib = r.getBib();
                                    //String time = DurationFormatter.durationToString(r.getFinishDuration().minus(r.getStartDuration()), "[HH:]MM:SS");
                                    ProcessedResult pr = processResult(r,race);
                                    String time = DurationFormatter.durationToString(pr.getChipFinish(), "[HH:]MM:SS");
                                    
                                    JSONObject json = new JSONObject();
                                    json.put("Bib", bib);
                                    json.put("Race", raceName);
                                    json.put("Time", time);
                                    HTTPServices.getInstance().publishEvent("RESULT", json);
                                }
                            });
                            pendingResults.forEach(r -> checkForCRQueue.add(r));
                            
                            Thread.sleep(100); 
                        } catch (InterruptedException ex) {
                            logger.debug("Interrupted",ex);
                        }
                        
                        
                    }

                }
            };
            Thread processNewResultThread = new Thread(processNewResult);
            processNewResultThread.setName("Thread-ProcessNewResultThread");
            processNewResultThread.setDaemon(true);
            processNewResultThread.setPriority(1);
            processNewResultThread.start();
            
            Task lookForCRs = new Task<Void>() {

                @Override 
                public Void call() {
                    Integer sleepTime = 15000; // 15 seconds
                    logger.debug("ResultsDAO: new CR checking thread started");
                    while(true) {
                        for(Race race: RaceDAO.getInstance().listRaces()){
                            if (race.getCourseRecords().size()>0) sleepTime = 1000; // reset the sleep time
                        }
                        try {
                            
                            List<Result> newResults = new ArrayList();
                            newResults.add(checkForCRQueue.take());
                            logger.debug("ResultsDAO LookforCRs Thread: Waiting for more results to process...");
                            
                            logger.debug("ResultsDAO LookforCRs Thread: The wait is over...");
                            Thread.sleep(100); // results rarely come in 1 at a time
                            checkForCRQueue.drainTo(newResults);

                            newResults.forEach(r -> {
                                RaceDAO.getInstance().getRaceByID(r.getRaceID()).getCourseRecords().forEach(cr -> {
                                    Result existing = cr.newRecord().get();
                                        cr.checkRecord(r);
                                    Result newRes = cr.newRecord().getValue();
                                    if (existing != null && (newRes == null || !existing.getBib().equals(newRes.getBib()))) {
                                        existing.getCourseRecords().remove(cr);
                                    }
                                });
                            });
                            
                            Thread.sleep(sleepTime); 
                        } catch (InterruptedException ex) {
                            logger.debug("Interrupted",ex);
                        }
                        
                        
                    }

                }
            };
            Thread lookForCRsThread = new Thread(lookForCRs);
            lookForCRsThread.setName("Thread-LookforCRsThread");
            lookForCRsThread.setDaemon(true);
            lookForCRsThread.setPriority(1);
            lookForCRsThread.start();
     
        }
    
        
        if(!raceResultsMap.containsKey(raceID)) raceResultsMap.put(raceID, FXCollections.observableArrayList(Result.extractor()));

        logger.trace("ResultsDAO.getResults for race ID " + raceID + " returning " + raceResultsMap.get(raceID).size());
        return raceResultsMap.get(raceID);
        
        
    }
    
    
    public void reprocessAllCRs(){
        raceDAO.listRaces().forEach(r -> reprocessAllCRs(r));
    }
    public void reprocessAllCRs(Race race){
        // Loop through all results for a race and match them up 
        // with the CR's.
        
        // Main goal is to make sure a result that was revised or removed 
        // is no longer listed as the new CR
        
        // This will do CRs x Results comparisions. 
        // No easy way to short circut this since we can't easily sort by segment
        // without looping throgh everything anyway or getting really complex
        // with pre-computing things to save half a second of time. 
        
        List<Result> results = getResults(race.getID());
        List<CourseRecord> crs = race.getCourseRecords();
        
        crs.forEach(cr -> {
            logger.debug("ResultsDAO::reprocessAllCRs: SegmentID=" + cr.getSegmentID() + " " + cr.getSex() + " " + cr.getCategory());
            //Result existing = cr.newRecord().get();
            
            cr.clearNewRecord();
            
            results.forEach(r -> {
                cr.checkRecord(r);
            });
            Result newRes = cr.newRecord().getValue();
//            if (existing != null && (newRes == null || !existing.getBib().equals(newRes.getBib()))) {
//                existing.getCourseRecords().remove(cr);
//            }
        });
        
    }
    
    public void clearResultsByParticipant(Participant p){
        
    }
        
        
    public void reprocessAllResults(){
        resultsQueue.addAll(resultsMap.keySet());
    }    
    
    public void reprocessWaveResults(Wave w) {
        participantDAO.listParticipants().stream().filter(p -> p.getWaveIDs().contains(w.getID())).forEach(p2 -> resultsQueue.add(p2.getBib()));
    }
    public void reprocessRaceResults(Race r) {
        r.getWaves().stream().forEach(w -> {reprocessWaveResults(w);});
    }
    
    // This is absolutely ugly. I hope it works... 
    private void processBib(String bib){
        logger.trace("ResultsDAO.processBib: " + bib);
        //List<Result> resultsList = new ArrayList<>();
        
        if (!resultsMap.containsKey(bib)) {
            resultsMap.put(bib,new HashMap());
        } else {
            resultsMap.get(bib).keySet().forEach(k -> {
                resultsMap.get(bib).get(k).clearTimes();
            });
        }
        
        Participant p = participantDAO.getParticipantByBib(bib);
        if (p == null) return;
        logger.trace("Processing " + p.fullNameProperty().getValueSafe());
        
        Set<Integer> waves = p.getWaveIDs();
        if (waves.isEmpty()) return;
        
        if (p.getStatus().equals(Status.DNS)) return; // They did not start
        
        Optional<List<TimeOverride>> bibOverrides = timingDAO.getOverridesByBib(bib);
        if (timingDAO.getCookedTimesByBib(bib).isEmpty() && ! bibOverrides.isPresent()) {
            return;
        }
        
        Map<Integer,Duration> overrideMap = new HashMap();
        if (bibOverrides.isPresent()) {
            bibOverrides.get().forEach(to -> {
                if (TimeOverrideType.OVERRIDE.equals(to.getOverrideType())) {
                    if (!to.getRelative()) {
                        overrideMap.put(to.getSplitId(), to.getTimestamp());
                        logger.debug("Override found for splitID of " + to.getSplitId() + " for " + to.getTimestamp());
                    } else {
                        // we have to convert the relative -> actual time
                        logger.debug("Relative override found for splitID of " + to.getSplitId() + " for " + to.getTimestamp());
                        overrideMap.put(to.getSplitId(), to.getTimestamp().negated());
                    }
                }
            });
        }
        
        
        //get a list of times and backup times
        List<CookedTimeData> allTimesList = new ArrayList(timingDAO.getCookedTimesByBib(bib)); 
        List<CookedTimeData> timesList = allTimesList.stream().filter(c -> !c.getBackupTime()).collect(Collectors.toList());
        List<CookedTimeData> backupTimesList = allTimesList.stream().filter(c -> c.getBackupTime()).collect(Collectors.toList());
        timesList.sort((p1, p2) -> p1.getTimestamp().compareTo(p2.getTimestamp()));
        backupTimesList.sort((p1, p2) -> p1.getTimestamp().compareTo(p2.getTimestamp()));
        
        logger.debug("ResultsDAO.processBib: " + bib + " we have " + timesList.size() + " times");
        for (CookedTimeData c: timesList){
            logger.debug(" " + c.getTimestamp().toString());
        }
        waves.forEach(i -> {
            logger.trace("Processing waveID " + i); 
            
            Boolean hasOverrides = false;
            Race race = raceDAO.getWaveByID(i).getRace();
            Result r = resultsMap.get(bib).get(race.getID());
            
            if (r == null ) {
                r = new Result();
                r.setBib(bib);
                r.setRaceID(race.getID());
                resultsMap.get(bib).put(r.getRaceID(), r);
            }
            
            
            
            Duration waveStart = Duration.between(LocalTime.MIDNIGHT, RaceDAO.getInstance().getWaveByID(i).waveStartProperty());
            Duration maxWaveStart = waveStart.plus(Duration.ofHours(1)); //FIX THIS!
            logger.trace("ResultsDAO.processBib: " + r.getBib() + " waveStart: " + waveStart + " maxWaveStart" + maxWaveStart);
            List<Split> splits = race.getSplits();
            
            r.setWaveStartDuration(waveStart);
            
            Iterator<CookedTimeData> times = timesList.iterator();
            Split[] splitArray = splits.toArray(new Split[splits.size()]); 
            Integer splitIndex = 0;
            
            Duration[] overrides = new Duration[splits.size()];
            for (int o = 0; o  < splits.size(); o++) {
                overrides[o] = overrideMap.get(splitArray[o].getID());
                if (overrides[o] != null) {
                    logger.trace("Found an override for split " + o + " of time " + overrides[o].toString());
                    hasOverrides = true;
                }
            }
            
            // do we have an override for the start?
            Boolean startTimeFound = false;
            if (hasOverrides && overrides[0] != null) {
                waveStart = overrides[0];
                r.setStartDuration(waveStart);
                r.setWaveStartDuration(waveStart);
                r.setSplitTime(0, waveStart);
                logger.trace("Found start time override of " + overrides[0].toString());
                
                // Adjust all relative overrides to actual
                for (int o = 1; o < splits.size(); o++) {
                    if (overrides[o] != null && overrides[o].isNegative()) {
                        overrides[o] = waveStart.plus(overrides[o].negated());
                    }
                }
                
                startTimeFound = true;

                splitIndex = 1; 
            }
            
            // pre-fill any intermediate splits
//            if (hasOverrides) {
//                for (int o = 1; o < splits.size()-1; o++) {
//                    if (overrides[o] != null) {
//                        logger.debug("Found split time override of " + overrides[o].toString() + " for the " + o + " split");
//                        r.setSplitTime(o+1, overrides[o]);
//                    }
//                }
//            }
            
            // Finish overrides get taken care of after the main while loop.... 
            
            CookedTimeData ctd = null;
            if (times.hasNext()) ctd = times.next();
            
            
            
            while(ctd != null) {
                logger.debug("ResultsDAO.processBib: Looking at: " + r.getBib() + " " + ctd.getTimestamp());
                
                // is there an override time for a future split that is before 
                // the time in the ctd? If so, advance to the split after that 
                // and consume the times
                if (hasOverrides) {
                    
                    if (!startTimeFound && splitIndex > 0) {
                        // Adjust all relative overrides to actual
                        
                        for (int o = 1; o < splits.size(); o++) {
                            if (overrides[o] != null && overrides[o].isNegative()) {
                                overrides[o] = r.getStartDuration().plus(overrides[o].negated());
                            }
                        }
                        startTimeFound = true;
                    }
                    
                    for (int ot = splitIndex; ot < splits.size(); ot++) {
                        
                        // MIN_TIME_TO_SPLIT
                        Duration backWindowDuration = Duration.ofMinutes(5);
                        Duration forwardWindowDuration = Duration.ofMinutes(5);
                        if (ot <splits.size()-1 && !Duration.ZERO.equals(splitArray[ot+1].splitMinTimeDuration())) { forwardWindowDuration = splitArray[ot+1].splitMinTimeDuration();} 
                        if (!Duration.ZERO.equals(splitArray[ot].splitMinTimeDuration())) { backWindowDuration = splitArray[ot].splitMinTimeDuration();} 
                        
                        if (overrides[ot] != null && !overrides[ot].isNegative() && overrides[ot].minus(backWindowDuration).compareTo(ctd.getTimestamp()) < 0) {
                            splitIndex= ot;
                            r.setSplitTime(splitArray[splitIndex].getPosition(), overrides[splitIndex]);
                            logger.debug("Found an override for " + splitIndex + " that is too close to the current times");

                            
                            Duration splitMax = overrides[ot].plus(forwardWindowDuration); 

                            // now consume the rest of the hits at this split until we 
                            // hit the max time for this split
                            do { 
                                logger.debug("Tossing ctd from " + ctd.getTimingLocationId() + " at " + ctd.getTimestamp());
                                if (times.hasNext()) ctd = times.next();
                                else ctd = null;
                            } while (ctd != null && ctd.getTimestamp().compareTo(splitMax) < 0 );
                            splitIndex++;
                            logger.debug("splitIndex now set to " + splitIndex);
                        }
                    }
                } 
                
                
                if (ctd == null || splitIndex >= splits.size()) {
                    break; // we ate all of the cooked times
                } else if (hasOverrides && overrides[splitIndex] != null) {
                    logger.trace("We have an override for " + splitIndex + " incrementing and moving on.");
                    r.setSplitTime(splitArray[splitIndex].getPosition(), overrides[splitIndex]);
                    
                    // MIN_TIME_TO_SPLIT
                    Duration forwardWindowDuration = Duration.ofMinutes(5);
                    if (splitIndex <splits.size()-1 && !Duration.ZERO.equals(splitArray[splitIndex+1].splitMinTimeDuration())) { forwardWindowDuration = splitArray[splitIndex+1].splitMinTimeDuration();} 
                        
                    Duration splitMax = overrides[splitIndex].plus(forwardWindowDuration); 
                    while (ctd != null && splitMax.compareTo(ctd.getTimestamp()) > 0 ) {
                        if (times.hasNext()) ctd = times.next();
                        else ctd = null;
                    }
                    
                    splitIndex++; // we pre-filled the split times earlier
                } else if (ctd.getTimestamp().compareTo(waveStart) < 0 ) {
                    logger.debug("ResultsDAO.processBib: tossing ctd's that were before the wave start");
                    // This will eat all times before the start time.
                    while (ctd != null && ctd.getTimestamp().compareTo(waveStart) < 0) {
                        logger.debug("Tossing pre-start time of " + ctd.getTimestamp().toString());
                        if (splitIndex == 0 && ctd.getTimingLocationId().equals(splitArray[splitIndex].getTimingLocationID())) r.setSplitTime(splitIndex, ctd.getTimestamp());
                        if (times.hasNext()) ctd = times.next();
                        else ctd = null;
                    }
                    
                    // If the next CTD is not for the start line... start tossing up to the min time to the first split. 
                    if (ctd != null && splitIndex == 0 && !ctd.getTimingLocationId().equals(splitArray[splitIndex].getTimingLocationID())){
                        
                        // We don't really have a start time, so eat any times toward the next split under the min_tim_to_split value
                        
                        // MIN_TIME_TO_SPLIT
                        Duration forwardWindowDuration = Duration.ofMinutes(5);
                        if (splitIndex <splits.size()-1 && !Duration.ZERO.equals(splitArray[splitIndex+1].splitMinTimeDuration())) { forwardWindowDuration = splitArray[splitIndex+1].splitMinTimeDuration();} 

                        Duration splitMax = waveStart.plus(forwardWindowDuration);  
                        logger.debug("Start forward splitMax is now " + DurationFormatter.durationToString(splitMax));
                        // now consume the rest of the hits at this split until we 
                        // either hit another location or hit the max
                        while (ctd != null && ctd.getTimestamp().compareTo(splitMax) < 0 ) { 
                            logger.debug("  Tossing " + ctd.getTimestamp().toString());
                            if (times.hasNext()) ctd = times.next();
                            else ctd = null;
                        } ;
                        
                    }
                    
                } else if (Objects.equals(ctd.getTimingLocationId(), splitArray[splitIndex].getTimingLocationID())) {
                    logger.trace("ResultsDAO.processBib: timing Location ID's match!");

                    if (splitIndex == 0) { // start line

                         // consume times, record the last one up to maxWaveStart 
                         // or we find a different locationID
                        do { 
                            logger.trace("ResultsDAO.processBib: start time: " + ctd.getTimestamp());
                            r.setSplitTime(splitIndex, ctd.getTimestamp());
                            r.setStartDuration(ctd.getTimestamp());
                            if (times.hasNext()) ctd = times.next();
                            else ctd = null;
                        } while (ctd != null && (ctd.getTimingLocationId() == splitArray[splitIndex].getTimingLocationID() && ctd.getTimestamp().compareTo(maxWaveStart) < 0) );
                    
                        // MIN_TIME_TO_SPLIT
                        Duration forwardWindowDuration = Duration.ofMinutes(5);
                        if (splitIndex <splits.size()-1 && !Duration.ZERO.equals(splitArray[splitIndex+1].splitMinTimeDuration())) { forwardWindowDuration = splitArray[splitIndex+1].splitMinTimeDuration();} 

                        Duration splitMax = r.getStartDuration().plus(forwardWindowDuration);  
                        logger.debug("Start forward splitMax is now " + DurationFormatter.durationToString(splitMax));
                        // now consume the rest of the hits at this split until we 
                        // either hit another location or hit the max
                        while (ctd != null && ctd.getTimestamp().compareTo(splitMax) < 0 ){ 
                            logger.debug("  Tossing " + ctd.getTimestamp().toString() + ": less than the splitMax");
                            if (times.hasNext()) ctd = times.next();
                            else ctd = null;
                        } ;
                    
                    } else if (splitIndex == splits.size() -1 ) { // finish line
                        logger.trace("We made it to the finish line!");
                        r.setFinishDuration(ctd.getTimestamp());
                        break; // we are done!
                    } else {
                        // we matched a split. 
                        logger.debug("We are at split " + splitIndex + " in " + ctd.getTimestamp());
                        r.setSplitTime(splitArray[splitIndex].getPosition(), ctd.getTimestamp());
                        
                        // MIN_TIME_TO_SPLIT
                        Duration forwardWindowDuration = Duration.ofMinutes(5);
                        if (splitIndex <splits.size()-1 && !Duration.ZERO.equals(splitArray[splitIndex+1].splitMinTimeDuration())) { forwardWindowDuration = splitArray[splitIndex+1].splitMinTimeDuration();} 

                        Duration splitMax = ctd.getTimestamp().plus(forwardWindowDuration);  
                        
                        // now consume the rest of the hits at this split until we 
                        // either hit another location or hit the max
                        do { 
                            if (times.hasNext()) ctd = times.next();
                            else ctd = null;
                        } while (ctd != null && ctd.getTimestamp().compareTo(splitMax) < 0 );
                        splitIndex++;
                    }
                } else { // walk the splitArray until we get a match
                     logger.debug("   Bumping the split index up from " + splitIndex);
                    Integer orgIndex  = splitIndex;
                    while (splitIndex < splits.size() && ctd.getTimingLocationId() != splitArray[splitIndex].getTimingLocationID()) splitIndex++;
                     logger.debug("      to " + splitIndex);
                    
                    if (splitIndex == splits.size()) {
                         logger.debug("oops, we hit the bottom, reset the splitIndex to " + orgIndex);
                        // Ok, so the current timing location is never used again. Odds are they just sat around
                        // there too long. Let's fix that
                        splitIndex = orgIndex;
                        Integer orgCTDLocation = ctd.getTimingLocationId() ;
                        do { 
                            if (times.hasNext()) ctd = times.next();
                            else ctd = null;
                        } while (ctd != null && orgCTDLocation == ctd.getTimingLocationId());
                    }
                    
                }
                
                if (splitIndex >= splits.size()) break; // failsafe to make sure we didn't run out of splits
                
            }
            
            // we processed every time so far
            
            // This should have been done before _unless_ there were no chip times
            for (int o = 1; o < splits.size(); o++) {
                if (overrides[o] != null && overrides[o].isNegative()) {
                    overrides[o] = r.getStartDuration().plus(overrides[o].negated());
                }
            }
            
            // fill any intermediate splits
            // ibid
            if (hasOverrides) {
                for (int o = 1; o < splits.size()-1; o++) {
                    if (overrides[o] != null) {
                        logger.trace("Found split time override of " + overrides[o].toString() + " for the " + o + " split");
                        r.setSplitTime(o+1, overrides[o]);
                    }
                }
            }

            // do we have an override for the finish?
            if (overrides[splits.size()-1] != null) {
                
                // Adjust all relative overrides to actual
                if (overrides[splits.size()-1].isNegative()) {
                    overrides[splits.size()-1] = r.getStartDuration().plus(overrides[splits.size()-1].negated());
                }
                logger.trace("Found finish time override of " + overrides[splits.size()-1].toString());
                r.setFinishDuration(overrides[splits.size()-1]);
            }
            
            // Now we are going to walk the times and look for backp times that may be able to fill the gaps.
            logger.trace("ResultsDAO.processBib: Result: " + r.getBib() + " " + r.getStartDuration() + " -> " + r.getFinishDuration());
            logger.trace("Backup time processing for bib " + p.getBib() + ". There are " + backupTimesList.size() + " backup reads.");
            
            if (backupTimesList.isEmpty()) return;
            int backupTimeIndex = 0;
            int maxBackupTimes = backupTimesList.size();
            CookedTimeData c = backupTimesList.get(backupTimeIndex);; 
            
            //fix the start time
            if(r.getStartDuration().equals(r.getWaveStartDuration())) {
                // zero gun time, look for a backup
                Duration maxStart = maxWaveStart;
                
                // If we have a 1st split time and there is a minimum time to that split, 
                // use that to box in the max allowed start time. 
                if (!r.getSplitTime(1).isZero() && !Duration.ZERO.equals(splitArray[1].splitMinTimeDuration())) { maxStart = r.getSplitTime(1).minus(splitArray[1].splitMinTimeDuration());} 

                
                while (Objects.equals(c.getTimingLocationId(), splitArray[0].getTimingLocationID()) && c.getTimestamp().compareTo(maxStart) < 0) {
                    if (r.getStartDuration().compareTo(c.getTimestamp())<0) {
                        r.setSplitTime(0, c.getTimestamp());
                        r.setStartDuration(c.getTimestamp());
                        logger.trace("Backup start time found for bib " + p.getBib());
                    }
                    if (backupTimeIndex < maxBackupTimes ) c = backupTimesList.get(backupTimeIndex++);
                    else break;
                }
            }
            
            // now fix any intermediate splits
            
            Duration lastSeen = r.getStartDuration();
            //todo next: adjust intermediate splits... 
            final Participant pa = p;
            final Result re = r;
            r.getSplitMap().keySet().forEach(k -> {
                //.out.println("Bib " + pa.getBib() + " split " + k + " is " + re.getSplitMap().get(k));
            });
            
            for (int si = 2; backupTimeIndex < maxBackupTimes  && si < splits.size() ; si++){
                logger.trace("Evaluating si " + si + " for bib "+ p.getBib());
                if (!r.getSplitTime(si).isZero() ) {
                    
                    // MIN_TIME_TO_SPLIT
                    Duration forwardWindowDuration = Duration.ofMinutes(5);
                    if (splitIndex <splits.size()-1 && !Duration.ZERO.equals(splitArray[splitIndex+1].splitMinTimeDuration())) { forwardWindowDuration = splitArray[splitIndex+1].splitMinTimeDuration();} 

                    lastSeen = r.getSplitTime(si).plus(forwardWindowDuration); 
                    logger.trace(" Previously seen at si " + si + " at " + lastSeen);

                    // consume any backup times older than this
                    while (lastSeen.compareTo(c.getTimestamp()) > 0){
                        logger.trace(" Tossing backup time at " + c.getTimestamp());
                        if (backupTimeIndex < maxBackupTimes ) c = backupTimesList.get(backupTimeIndex++);
                        else break;
                    }
                } else { //we have a blank split
                    logger.trace(" Not seen at si " + si + " last seen at " + lastSeen);
                    Duration nextSeen = Duration.ZERO;
                    for (int n = si+1; nextSeen.isZero() && n < splits.size()-1; n++){
                        nextSeen = r.getSplitTime(n);
                    }
                    if (nextSeen.isZero()) r.getFinishDuration(); 
                    logger.trace(" Seen next at " + nextSeen);

                    while (r.getSplitTime(si).isZero() ){
                        if (Objects.equals(c.getTimingLocationId(), splitArray[si-1].getTimingLocationID()) ) {
                            logger.debug(" Split time found for bib " + p.getBib() + " " + c.getTimestamp() + " " + lastSeen + " " + nextSeen);
                            
                            // MIN_TIME_TO_SPLIT
                            Duration backWindowDuration = Duration.ofMinutes(5);
                             if (splitIndex <splits.size()-1 && !Duration.ZERO.equals(splitArray[splitIndex+1].splitMinTimeDuration())) { backWindowDuration = splitArray[splitIndex+1].splitMinTimeDuration();} 

                            if (c.getTimestamp().compareTo(lastSeen) > 0  && (nextSeen.isZero() || c.getTimestamp().compareTo(nextSeen.minus(backWindowDuration)) < 0)) {
                                r.setSplitTime(si,c.getTimestamp());
                                lastSeen=c.getTimestamp();
                               logger.trace(" Split match found for bib " + p.getBib() + " at " + lastSeen);
                            }
                        }
                        if (backupTimeIndex < maxBackupTimes ) c = backupTimesList.get(backupTimeIndex++);
                        else { 
                            //we hit the bottom, reset the backupTimeIndex to zero to restart our search
                            logger.debug(" Hit backup bottom, restarting search");
                            backupTimeIndex=0;
                            break;
                        };
                    }
                }
            }
            
            
            if (backupTimeIndex == maxBackupTimes ) {
                logger.debug("No more backup times found for bib " + p.getBib());
                return; // out of backup times
            } 
            
            // now fix the finish time 
            if (r.getFinishDuration() == null || r.getFinishDuration().isZero()) { // we need a finish time
                int finishSplit = splits.size()-1;
                
                // find the last time we saw this runner
                for (int si = 1; si < finishSplit; si++ ){
                    if (r.getSplitTime(si) != Duration.ZERO) lastSeen = r.getSplitTime(si);
                }
                backupTimeIndex=0;
                c = backupTimesList.get(backupTimeIndex++);
                while (r.getFinishDuration() == null || r.getFinishDuration().isZero() ){
                    if (Objects.equals(c.getTimingLocationId(), splitArray[finishSplit].getTimingLocationID()) ) {
                        if (c.getTimestamp().compareTo(lastSeen) > 0) r.setFinishDuration(c.getTimestamp());
                        logger.debug("Backup finish time found for bib " + p.getBib());
                    }
                    if (backupTimeIndex < maxBackupTimes ) c = backupTimesList.get(backupTimeIndex++);
                    else break;
                }
            }
            
            logger.debug("ResultsDAO.processBib: Final Result: " + r.getBib() + " " + r.getStartDuration() + " -> " + r.getFinishDuration());
            //resultsList.add(r); 
            //resultsMap.put(bib + " " + r.getRaceID(), r);
            
        });
        
    }
    
    
    
    
    public void saveRaceReport(RaceReport rr){
        Session s=HibernateUtil.getSessionFactory().getCurrentSession();
        s.beginTransaction();
        s.saveOrUpdate(rr);
        s.getTransaction().commit();
    }
            
    public void removeRaceReport(RaceReport rr){
        Session s=HibernateUtil.getSessionFactory().getCurrentSession();
        s.beginTransaction();
        s.delete(rr);
        s.getTransaction().commit(); 
    }
    
    public void saveReportDestination(ReportDestination p) {
        Session s=HibernateUtil.getSessionFactory().getCurrentSession();
        s.beginTransaction();
        s.saveOrUpdate(p);
        s.getTransaction().commit();
        //Platform.runLater(() -> {
        if (!reportDestinationList.contains(p)) reportDestinationList.add(p);
        //});
        
    }
    
    public void refreshReportDestinationList() { 
        List<ReportDestination> list = new ArrayList<>();
        
        reportDestinationListInitialized.setValue(TRUE);

        Session s=HibernateUtil.getSessionFactory().getCurrentSession();
        s.beginTransaction();
        logger.debug("Runing the refreshReportDestinationList Query");

        try {  
            list=s.createQuery("from ReportDestination").list();
        } catch (Exception e) {
            logger.debug("refreshReportDestinationList(): {}",e.getMessage(),e);
            //e.printStackTrace();
        } 
        s.getTransaction().commit(); 

        logger.debug("Returning the refreshReportDestinationList list: " + list.size());
        reportDestinationList.addAll(list);   

    }     
    
    public ObservableList<ReportDestination> listReportDestinations() { 

        if (!reportDestinationListInitialized.get()) refreshReportDestinationList();
        return reportDestinationList;
        //return list;
    }     
    
    public ReportDestination getReportDestinationByUUID(String id) {
        logger.trace("Looking for a timingLocation with id " + id);
        // This is ugly. Setup a map for faster lookups
        if (!reportDestinationListInitialized.get()) refreshReportDestinationList();
        Optional<ReportDestination> result = reportDestinationList.stream()
                    .filter(t -> Objects.equals(t.getUUID(), id))
                    .findFirst();
        if (result.isPresent()) {
            logger.trace("Found " + result.get().getName());
            return result.get();
        } 
        
        return null;
    }
    
    public ReportDestination getReportDestinationByID(Integer id) {
        logger.trace("Looking for a timingLocation with id " + id);
        // This is ugly. Setup a map for faster lookups
        if (!reportDestinationListInitialized.get()) refreshReportDestinationList();
        Optional<ReportDestination> result = reportDestinationList.stream()
                    .filter(t -> Objects.equals(t.getID(), id))
                    .findFirst();
        if (result.isPresent()) {
            logger.trace("Found " + result.get().getName());
            return result.get();
        } 
        
        return null;
    }

    public void removeReportDestination(ReportDestination op) {
        
        Session s=HibernateUtil.getSessionFactory().getCurrentSession();
        s.beginTransaction();
        s.delete(op);
        s.getTransaction().commit(); 
        reportDestinationList.remove(op);
    }
    
    
    
    public void saveRaceReportOutputTarget(RaceOutputTarget t) {
        Session s=HibernateUtil.getSessionFactory().getCurrentSession();
        s.beginTransaction();
        s.saveOrUpdate(t);
        s.getTransaction().commit(); 
    }

    public void removeRaceReportOutputTarget(RaceOutputTarget t) {
        Session s=HibernateUtil.getSessionFactory().getCurrentSession();
        s.beginTransaction();
        s.delete(t);
        s.getTransaction().commit(); 
    }
    
    
    
    public void processAllReports(){
        raceDAO.listRaces().forEach(r -> {processReports(r, null);});
    }
    
    public void processReport(RaceReport rr) {
        processReports(rr.getRace(),rr);
    }
    
    public ProcessedResult processResult(Result res, Race race){
        ProcessedResult pr = new ProcessedResult();
        Integer splitSize = race.getSplits().size();
                        
        // Link in the participant
        pr.setParticipant(participantDAO.getParticipantByBib(res.getBib()));
        // Set the AG code (e.g. M30-34) (age and gender are set automagically)
        pr.setAge(pr.getParticipant().getAge());
        pr.setAGCode(race.getAgeGroups().ageToAGString(pr.getAge()));

        // set the start and wave start times
        Duration chipStartTime = res.getStartDuration();
        Duration waveStartTime = res.getWaveStartDuration();

        // Set the start duration
        pr.setChipStartTime(chipStartTime);
        pr.setWaveStartTime(waveStartTime);

        // by definition, you cross the start line at zero seconds
        pr.setSplit(1, Duration.ZERO);

        //if(chipStartTime.equals(waveStartTime)) logger.debug("Chip == Wave Start for " + res.getBib());

        // if they are DQ'ed then we don't care what their time is
        if (pr.getParticipant().getDQ()) {
            //results.add(pr);
            return pr;
        }


        // Set the splits
        Duration paused = Duration.ZERO;
        Boolean missingSplit = false;
        if(race.getSplits().size() > 2) {
            for (int i = 2; i <  splitSize ; i++) {
                logger.trace("Split: " + race.getSplits().get(i-1).getSplitName() + " Ignore? " + race.getSplits().get(i-1).getIgnoreTime() );
                if (race.getSplits().get(i-1).getIgnoreTime() && !res.getSplitTime(i).isZero()) {
                    if (i == 2) paused = res.getSplitTime(i).minus(chipStartTime);
                    else if (!res.getSplitTime(i-1).isZero()) paused = paused.plus(res.getSplitTime(i).minus(res.getSplitTime(i-1)));
                    logger.debug("Paused time for " + pr.getParticipant().getBib() + " " + paused + " from " + res.getSplitTime(i)+ " minus " + res.getSplitTime(i-1) );
                }
                if (! res.getSplitTime(i).isZero()) { 
                    pr.setSplit(i,res.getSplitTime(i).minus(chipStartTime).minus(paused));
                    pr.setSplitTOD(i, res.getSplitTime(i));
                }

                // Is this a mandatory split that we are missing?
                if (race.getSplits().get(i-1).getMandatorySplit() && (pr.getSplit(i) == null || pr.getSplit(i).isZero())){
                    // Mandatory split: Stop right here
                    //results.add(pr);
                    return pr;
                }
                // Check to see if we are over a cutoff for this split. 
                if (! res.getSplitTime(i).isZero() && !Duration.ZERO.equals(race.getSplits().get(i-1).splitCutoffDuration())){
                    if (race.getSplits().get(i-1).getSplitCutoffIsRelative()) {
                        if (pr.getSplit(i).compareTo(race.getSplits().get(i-1).splitCutoffDuration()) > 0 ) {
                            pr.oco = TRUE;
                            pr.ocoSplit = race.getSplits().get(i-1).getID();
                            pr.ocoTime = pr.getSplit(i);
                            pr.ocoCutoffTime = race.getSplits().get(i-1).splitCutoffDuration();
                            //results.add(pr);
                            return pr;
                        }
                    } else {
                        if (res.getSplitTime(i).compareTo(race.getSplits().get(i-1).splitCutoffDuration()) > 0 ) {
                            pr.oco = TRUE;
                            pr.ocoSplit = race.getSplits().get(i-1).getID();
                            pr.ocoTime = res.getSplitTime(i);
                            pr.ocoCutoffTime = race.getSplits().get(i-1).splitCutoffDuration();
                            //results.add(pr);
                            return pr;
                        }
                    }
                }
            }
        }

        // Set the finish times unless they are a DNF
        if(res.getFinishDuration() != null && ! res.getFinishDuration().isZero() && ! pr.getParticipant().getDNF()){
            pr.setChipFinish(res.getFinishDuration().minus(chipStartTime).minus(paused));
            pr.setGunFinish(res.getFinishDuration().minus(waveStartTime).minus(paused));
            pr.setSplit(splitSize, pr.getChipFinish());
            pr.setFinishTOD(res.finishTODProperty().getValue());
        }

        // look for any bonus or penalty times
        Optional<List<TimeOverride>> overrides = timingDAO.getOverridesByBib(pr.getParticipant().getBib());
        if (overrides.isPresent() && pr.getChipFinish() != null) {
            overrides.get().forEach(o -> {
                if (TimeOverrideType.PENALTY.equals(o.getOverrideType())){
                    pr.penalty = true;
                    pr.penaltyTime = Duration.ofNanos(o.getTimestampLong());
                    pr.bonusPenaltyNote = o.getNote();
                    pr.rawChipFinishTime=pr.getChipFinish();
                    pr.rawGunFinishTime=pr.getGunFinish();
                    pr.setChipFinish(pr.getChipFinish().plus(pr.penaltyTime));
                    pr.setGunFinish(pr.getGunFinish().plus(pr.penaltyTime));
                } else if (TimeOverrideType.BONUS.equals(o.getOverrideType())){
                    pr.bonus = true;
                    pr.bonusTime = Duration.ofNanos(o.getTimestampLong());
                    pr.bonusPenaltyNote = o.getNote();
                    pr.rawChipFinishTime=pr.getChipFinish();
                    pr.rawGunFinishTime=pr.getGunFinish();
                    pr.setChipFinish(pr.getChipFinish().minus(pr.bonusTime));
                    pr.setGunFinish(pr.getGunFinish().minus(pr.bonusTime));
                } 

            });
        }

        // set the segment times unless they are a DNF
        if (!pr.getParticipant().getDNF()) race.getSegments().forEach(seg -> {
            logger.trace("Processing segment " + seg.getSegmentName());
            if (pr.getSplit(seg.getEndSplitPosition()) != null && pr.getSplit(seg.getStartSplitPosition()) != null) {
                pr.setSegmentTime(seg.getID(), pr.getSplit(seg.getEndSplitPosition()).minus(pr.getSplit(seg.getStartSplitPosition())));
                logger.trace("Segment: Bib " + pr.getParticipant().getBib() + " Segment " + seg.getSegmentName() + " Time " + DurationFormatter.durationToString(pr.getSegmentTime(seg.getID())));
            }
        });
        
        pr.setCourseRecords(res.getCourseRecords());

        return pr;
    }
    public void processReports(Race r, RaceReport rr){
        
        Task processRaceReports;
        processRaceReports = new Task<Void>() {
            
            @Override
            public Void call() {
                try{
                    List<ProcessedResult> results = new ArrayList();

                    // get the current results list
                    getResults(r.getID()).forEach(res -> {
                        
                        // If there is no participant, then bail. 
                        // TODO: Maybe add an option to create a participant on the fly, but
                        // this could gete messy with all of the random RFID chips out there.
                        // Either way, this would be handled on the timing tab, not here.
                        if(participantDAO.getParticipantByBib(res.getBib()) == null) return;
                        
                        ProcessedResult pr = processResult(res,r);
                        results.add(pr);
                    });

                    // sort it by finish, then last completed split
                    results.sort(null); // ProcessedResult iplements the Comparable interface

                    // calculate placement in Overall, Gender, AG
                    Map<String,Integer> placementCounter = new HashMap();
                    placementCounter.put("overall", 1);


                    results.forEach(pr -> {
                        pr.setOverall(placementCounter.get("overall"));
                        placementCounter.put("overall", pr.getOverall()+1);
                       
                        placementCounter.putIfAbsent(pr.getSex(), 1);
                        pr.setSexPlace(placementCounter.get(pr.getSex()));
                        placementCounter.put(pr.getSex(), pr.getSexPlace()+1);

                        placementCounter.putIfAbsent(pr.getSex()+pr.getAGCode(), 1);
                        pr.setAGPlace(placementCounter.get(pr.getSex()+pr.getAGCode()));
                        placementCounter.put(pr.getSex()+pr.getAGCode(),pr.getAGPlace()+1);
                    });
                    
                    // now do the same for segments 
                    r.getSegments().forEach(seg -> {
                        results.sort((p1, p2) -> {
                            return ObjectUtils.compare(p1.getSegmentTime(seg.getID()), p2.getSegmentTime(seg.getID()));
                        });
                        Map<String,Integer> segPlCounter = new HashMap();
                        segPlCounter.put("overall", 1);

                        
                        results.forEach(pr -> {
                            if (pr.getSegmentTime(seg.getID()) == null) return;
                            logger.trace("Segment " + seg.getID() + " runner: " + pr.participant.fullNameProperty().get() + " time: " + DurationFormatter.durationToString(pr.getSegmentTime(seg.getID()),r.getStringAttribute("TimeDisplayFormat"),r.getStringAttribute("TimeRoundingMode")));
                            logger.trace("  This: Overall: " + segPlCounter.get("overall") + " Sex: " + segPlCounter.get(pr.getSex()) );
                            
                            pr.setSegmentOverallPlace(seg.getID(),segPlCounter.get("overall"));
                            segPlCounter.put("overall", segPlCounter.get("overall")+1);
                            
                            segPlCounter.putIfAbsent(pr.getSex(), 1);
                            pr.setSegmentSexPlace(seg.getID(),segPlCounter.get(pr.getSex()));
                            segPlCounter.put(pr.getSex(), segPlCounter.get(pr.getSex())+1);

                            segPlCounter.putIfAbsent(pr.getSex()+pr.getAGCode(), 1);
                            pr.setSegmentAGPlace(seg.getID(),segPlCounter.get(pr.getSex()+pr.getAGCode()));
                            segPlCounter.put(pr.getSex()+pr.getAGCode(),segPlCounter.get(pr.getSex()+pr.getAGCode())+1);
                            logger.trace("  Next: Overall: " + segPlCounter.get("overall") + " Sex: " + segPlCounter.get(pr.getSex()) );
                        });
                    });
                    
                    // now sort them again
                    results.sort(null); 
                    
                    // Now deal with ties. Ugh.
                    if (r.getBooleanAttribute("permitTies") != null && r.getBooleanAttribute("permitTies") && r.getStringAttribute("TimeDisplayFormat") != null) {
                        String dispFormat = r.getStringAttribute("TimeDisplayFormat");
                        String roundMode = r.getStringAttribute("TimeRoundingMode");
                        
                        logger.debug("Tie Processing: Display Format: " + dispFormat + " Rounding " + roundMode);
                        
                        // Overall ties
                        StringProperty lastResult = new SimpleStringProperty(""); 
                        placementCounter.clear();
                        

                        results.forEach(pr -> {
                            if (pr.getChipFinish() == null) return;
                            String currentResult = DurationFormatter.durationToString(pr.getChipFinish(),dispFormat,roundMode);
                            
                            if (currentResult.equals(lastResult.getValueSafe())) { // we have a tie
                                logger.debug("We have tie at " + currentResult);
                                pr.setOverall(placementCounter.get("overall"));
                                
                                placementCounter.putIfAbsent(pr.getSex(), pr.getSexPlace());
                                pr.setSexPlace(placementCounter.get(pr.getSex()));
                                
                                placementCounter.putIfAbsent(pr.getSex()+pr.getAGCode(),pr.getAGPlace());
                                pr.setAGPlace(placementCounter.get(pr.getSex()+pr.getAGCode()));
                            } else {
                                lastResult.set(currentResult);
                                placementCounter.clear();
                                placementCounter.put("overall", pr.getOverall());
                                placementCounter.put(pr.getSex(), pr.getSexPlace());
                                placementCounter.put(pr.getSex()+pr.getAGCode(),pr.getAGPlace());
                            }

                        });
                        
                        // segment ties
                        r.getSegments().forEach(seg -> {
                            lastResult.set("");
                            results.sort((p1, p2) -> {
                                return ObjectUtils.compare(p1.getSegmentTime(seg.getID()), p2.getSegmentTime(seg.getID()));
                            });
                            results.forEach(pr -> {
                                if (pr.getSegmentTime(seg.getID()) == null) return;
                                
                                String currentResult = DurationFormatter.durationToString(pr.getSegmentTime(seg.getID()),dispFormat,roundMode);

                                if (currentResult.equals(lastResult.getValueSafe())) { // we have a tie
                                    logger.debug("We have a segment tie at " + currentResult + " for segID " + seg.getID());
                                    pr.setSegmentOverallPlace(seg.getID(),placementCounter.get("overall"));

                                    placementCounter.putIfAbsent(pr.getSex(), pr.getSegmentSexPlace(seg.getID()));
                                    pr.setSegmentSexPlace(seg.getID(),placementCounter.get(pr.getSex()));

                                    placementCounter.putIfAbsent(pr.getSex()+pr.getAGCode(),pr.getSegmentAGPlace(seg.getID()));
                                    pr.setSegmentAGPlace(seg.getID(),placementCounter.get(pr.getSex()+pr.getAGCode()));
                                } else {
                                    lastResult.set(currentResult);
                                    placementCounter.clear();
                                    placementCounter.put("overall", pr.getSegmentOverallPlace(seg.getID()));
                                    placementCounter.put(pr.getSex(), pr.getSegmentSexPlace(seg.getID()));
                                    placementCounter.put(pr.getSex()+pr.getAGCode(),pr.getSegmentAGPlace(seg.getID()));
                                }

                            });
                        });
                        
                        // now sort them again
                        results.sort(null); 
                    }
                    
                    
                    // for each report, feed it the results list
                    if (rr == null) {
                        r.raceReportsProperty().forEach(rr ->{
                            rr.processResultIfEnabled(results);
                        }); 
                    } else rr.processResultNow(results);
                } catch (Exception ex){
                    ex.printStackTrace();
                }
                return null;
            }
        };
            Thread processNewResultThread = new Thread(processRaceReports);
            processNewResultThread.setName("Thread-processRaceReports-" + r.getRaceName());
            processNewResultThread.setDaemon(true);
            processNewResultThread.setPriority(1);
            processNewResultThread.start();
        
    }
    
    
    
}
