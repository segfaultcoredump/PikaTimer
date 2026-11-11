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

import org.pikatimer.results.ResultsDAO;
import org.pikatimer.util.FxBulk;
import org.pikatimer.util.HibernateUtil;
import java.util.ArrayList;
import java.util.Collection;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import javafx.application.Platform;
//import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import org.hibernate.query.Query;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jcgarner
 */
public class TimingDAO {
    private static final Logger logger = LoggerFactory.getLogger(TimingDAO.class);
    
    private static final ObservableList<TimingLocation> timingLocationList =FXCollections.observableArrayList(TimingLocation.extractor());
    private static final BlockingQueue<RawTimeData> rawTimeQueue = new ArrayBlockingQueue(100000);
    private static final BlockingQueue<CookedTimeData> cookedTimeQueue = new ArrayBlockingQueue(100000);
    private static final ResultsDAO resultsDAO = ResultsDAO.getInstance();
    private static final BlockingQueue<String> resultsQueue = ResultsDAO.getInstance().getResultsQueue();

    private static ObservableList<CookedTimeData> cookedTimeList;
    private static final Map<String,List<CookedTimeData>> cookedTimeBibMap = new ConcurrentHashMap();
    private Bib2ChipMap bib2ChipMap;

    private static ObservableList<TimeOverride> overrideList;
    private  Map<String,List<TimeOverride>> overrideMap = new ConcurrentHashMap(); 

    // Held by whoever is persisting or replacing cooked times. Cooked times never 
    // leave the cookedTimeQueue unless this is held, so a bulk replace can 
    // reliably pull out anything that is still pending for the inputs it is redoing. 
    Semaphore cookedTimeProcessorSemaphore = new Semaphore(1);
    // One permit per "there is something in the cookedTimeQueue" notification
    private final Semaphore cookedTimeSignal = new Semaphore(0);
    
    // Same idea for the raw times
    private final Object rawTimeLock = new Object();
    private final Semaphore rawTimeSignal = new Semaphore(0);
    private boolean rawTimeWriterStarted = false;
    
    // Number of bulk operations (reprocess, clear) that are in flight
    private static final AtomicInteger bulkOperations = new AtomicInteger();
    private static final CountDownLatch locationsLoadedLatch = new CountDownLatch(1);

    // The lists in the cookedTimeBibMap are never modified once they are in the 
    // map. A change swaps in a new list, so a reader on another thread always
    // has a stable list to work with. 
    public List<CookedTimeData> getCookedTimesByBib(String bib) {
        List<CookedTimeData> times = cookedTimeBibMap.get(bib);
        if (times != null) return times; 
        return new ArrayList();
    }
    
    private void addToBibMap(CookedTimeData c) {
        cookedTimeBibMap.compute(c.getBib(), (bib, old) -> {
            List<CookedTimeData> updated = (old == null) ? new ArrayList() : new ArrayList(old);
            updated.add(c);
            return updated;
        });
    }
    
    private void removeFromBibMap(CookedTimeData c) {
        cookedTimeBibMap.computeIfPresent(c.getBib(), (bib, old) -> {
            List<CookedTimeData> updated = new ArrayList(old);
            updated.remove(c);
            return updated;
        });
    }
    
    // Removes everything that matches and returns the bibs that lost a time
    private Set<String> removeFromBibMap(Predicate<CookedTimeData> remove) {
        Set<String> bibs = new HashSet();
        cookedTimeBibMap.replaceAll((bib, old) -> {
            if (old.stream().noneMatch(remove)) return old;
            bibs.add(bib);
            List<CookedTimeData> updated = new ArrayList(old);
            updated.removeIf(remove);
            return updated;
        });
        return bibs;
    }
    
    /**
     * True while cooked times are still on their way: either a bulk operation 
     * is running or there are times waiting to be saved. The results 
     * processor uses this to avoid recalculating from a half finished picture. 
     */
    public boolean isBusy() {
        return bulkOperations.get() > 0 || !cookedTimeQueue.isEmpty();
    }
        
        
    /**
    * SingletonHolder is loaded on the first execution of Singleton.getInstance() 
    * or the first access to SingletonHolder.INSTANCE, not before.
    */
    private static class SingletonHolder { 
            private static final TimingDAO INSTANCE = new TimingDAO();
    }

    public static TimingDAO getInstance() {
            return SingletonHolder.INSTANCE;
    }
    
    public BlockingQueue<RawTimeData> getRawTimeQueue () {
        return rawTimeQueue; 
    }
    public Collection getRawTimes(TimingLocationInput tli) {
        logger.debug("Runing the getRawTimes Query for " + tli.getLocationName());
        
        return HibernateUtil.loadReadOnly("from RawTimeData where timingLocationInputId = :tli_id", Map.of("tli_id", tli.getID()));
    }
    
      
    /**
     * Queue a raw time to be saved. A background thread writes whatever has 
     * piled up in a single transaction, so importing a file with tens of 
     * thousands of reads is a handful of commits and not one per read. 
     */
    public void saveRawTimes(RawTimeData r) {
        startRawTimeWriter();
        try {
            rawTimeQueue.put(r); // blocks if the writer is way behind
            rawTimeSignal.release();
        } catch (InterruptedException ex) {
            logger.warn("Interrupted!",ex);
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Give the background writers a chance to finish saving anything that is 
     * still queued up. Called when the application is closing. 
     */
    public void flushPendingWrites() {
        long deadline = System.currentTimeMillis() + 5000;
        try {
            while (System.currentTimeMillis() < deadline) {
                if (rawTimeQueue.isEmpty() && cookedTimeQueue.isEmpty()) {
                    // Wait out a batch that is in the middle of being written
                    synchronized(rawTimeLock) { }
                    if (cookedTimeProcessorSemaphore.tryAcquire(Math.max(1, deadline - System.currentTimeMillis()), java.util.concurrent.TimeUnit.MILLISECONDS)) {
                        cookedTimeProcessorSemaphore.release();
                    }
                    if (rawTimeQueue.isEmpty() && cookedTimeQueue.isEmpty()) return;
                }
                Thread.sleep(20);
            }
            logger.warn("TimingDAO::flushPendingWrites: gave up waiting with " + rawTimeQueue.size() + " raw and " + cookedTimeQueue.size() + " cooked times unsaved");
        } catch (InterruptedException ex) {
            logger.warn("Interrupted!",ex);
        }
    }
    
    private synchronized void startRawTimeWriter() {
        if (rawTimeWriterStarted) return;
        rawTimeWriterStarted = true;
        
        Thread rawTimeWriterThread = new Thread(() -> {
            while(true) {
                try {
                    rawTimeSignal.acquire();
                    Thread.sleep(10); // Times rarely come in 1 at a time
                    rawTimeSignal.drainPermits();
                    
                    synchronized(rawTimeLock) {
                        List<RawTimeData> pending = new ArrayList();
                        rawTimeQueue.drainTo(pending);
                        if (pending.isEmpty()) continue;
                        logger.debug("RawTimeWriter Thread: Saving: " + pending.size());
                        
                        Session s=HibernateUtil.getSessionFactory().getCurrentSession();
                        s.beginTransaction();
                        try {
                            int count = 0;
                            for (RawTimeData r: pending) {
                                s.save(r);
                                if ( ++count % 100 == 0 ) {
                                    //flush a batch of updates and release memory:
                                    s.flush();
                                    s.clear();
                                }
                            }
                            s.getTransaction().commit();
                        } catch (Exception e) {
                            logger.error("RawTimeWriter Thread: Unable to save " + pending.size() + " raw times", e);
                            try { s.getTransaction().rollback(); } catch (Exception e2) { logger.debug("Rollback failed", e2); }
                        }
                    }
                } catch (InterruptedException ex) {
                    logger.warn("Interrupted!",ex);
                }
            }
        });
        rawTimeWriterThread.setName("Thread-RawTimeWriterThread");
        rawTimeWriterThread.setDaemon(true);
        rawTimeWriterThread.start();
    }
    
    public void clearRawTimes(TimingLocationInput tli) {
        // Holding the lock means the writer is not in the middle of a batch, and 
        // anything still waiting to be written for this input can be tossed
        synchronized(rawTimeLock) {
            rawTimeQueue.removeIf(r -> Objects.equals(r.getTimingLocationInputId(), tli.getID()));
            
            Session s=HibernateUtil.getSessionFactory().getCurrentSession();
            s.beginTransaction();
            logger.debug("Deleting all raw times for " + tli.getLocationName());

            try {  
                s.createQuery("delete from RawTimeData where timingLocationInputId = :tli_id").setParameter("tli_id", tli.getID()).executeUpdate();
            } catch (Exception e) {
                logger.debug(e.getMessage());
            } 
            s.getTransaction().commit(); 
        }
        
        tli.clearLocalReads();
        // Now go and clean up the cooked times too
        clearCookedTimes(tli); 
    }
    
    public void clearRawTimes(TimingLocation tl) {
        if (tl == null || tl.inputsProperty().isEmpty()) return;
        tl.getInputs().stream().forEach(tli -> {
            clearRawTimes(tli);
        });
    }
    
    
    
    public ObservableList<CookedTimeData> getCookedTimes() {
        
        // if the cooked time list is null, then let's create one,
        // fetch fetch existing datapoints from the DB,
        // and add them to the cookedTimesList;
        
        if (cookedTimeList == null) {
            cookedTimeList =FXCollections.observableArrayList(CookedTimeData.extractor());
            
            Task fetchCookedFromDB = new Task<Void>() {
                
                @Override public Void call() {
                   logger.debug("Runing the getCookedTimes querry");
                   final List<CookedTimeData> cookedTimes = HibernateUtil.loadReadOnly("from CookedTimeData");
                    
                    Platform.runLater(() -> {
                        cookedTimeList.addAll(cookedTimes);
                    });
                    Map<String,List<CookedTimeData>> loaded = new java.util.HashMap();
                    cookedTimes.stream().forEach(c -> {
                        List<CookedTimeData> bibTimes = loaded.computeIfAbsent(c.getBib(), k -> new ArrayList());
                        if (!c.ignoreTimeProperty().get()) bibTimes.add(c);
                    });
                    loaded.forEach((bib, times) -> cookedTimeBibMap.merge(bib, times, (a, b) -> {
                        List<CookedTimeData> both = new ArrayList(a);
                        both.addAll(b);
                        return both;
                    }));
                    
                    return null; 
                }
            }; 
            new Thread(fetchCookedFromDB).start();
            
            
        
            
            
            Task processNewCooked = new Task<Void>() {
                @Override public Void call() {
                    
                    while(true) {
                        List<CookedTimeData> pending = new ArrayList();
                        List<CookedTimeData> newTimes = new ArrayList();
                        boolean locked = false;
                        try {
                            // Wait for something to show up, but leave it in the queue 
                            // until we hold the semaphore. 
                            cookedTimeSignal.acquire();
                            Thread.sleep(10); // Times rarely come in 1 at a time
                            cookedTimeSignal.drainPermits();
                            
                            cookedTimeProcessorSemaphore.acquire();
                            locked = true;
                            
                            cookedTimeQueue.drainTo(pending);
                            if (pending.isEmpty()) continue;
                            logger.debug("ProcessNewCooked Thread: Processing: " + pending.size());

                            Session s=HibernateUtil.getSessionFactory().getCurrentSession();
                            s.beginTransaction();
                            int count = 0;
                            Iterator<CookedTimeData> addIterator = pending.iterator();
                            while (addIterator.hasNext()) {
                                CookedTimeData c = addIterator.next();
                                if (c.getID() == null) newTimes.add(c);
                                
                                s.saveOrUpdate(c); 
                                
                                if ( ++count % 100 == 0 ) {
                                    //flush a batch of updates and release memory:
                                    s.flush();
                                    s.clear();
                                }

                            }
                            s.getTransaction().commit(); 

                            Platform.runLater(() -> {
                                FxBulk.addAll(cookedTimeList, newTimes);
                            });
                            Set<String> bibs = new HashSet();
                            pending.stream().forEach(c -> {
                                if (c.ignoreTimeProperty().get()) removeFromBibMap(c);
                                else addToBibMap(c);
                                bibs.add(c.getBib());
                            });
                            resultsQueue.addAll(bibs);
                            
                        } catch (InterruptedException ex) {
                            logger.warn("Interrupted!",ex);
                        } catch (Exception ex) {
                            // Don't let one bad batch kill the processing thread
                            logger.error("ProcessNewCooked Thread: Unable to process " + pending.size() + " times", ex);
                        } finally {
                            if (locked) cookedTimeProcessorSemaphore.release();
                        }
                        
                        try {
                            Thread.sleep(100); // Let more times pile up so we work in batches
                        } catch (InterruptedException ex) {
                            logger.warn("Interrupted!",ex);
                        }
                    }

                }
            };
            Thread processNewCookedThread = new Thread(processNewCooked);
            processNewCookedThread.setName("Thread-ProcessNewCookedThread");
            processNewCookedThread.setDaemon(true);
            processNewCookedThread.setPriority(1);
            processNewCookedThread.start();
        }
    
        return cookedTimeList;
        
        
    }
    
    public void saveCookedTime(CookedTimeData c) {
        cookedTimeQueue.add(c); 
        cookedTimeSignal.release();
    }
    
    public void clearAllCookedTimes() {
        Task clearTimes = new Task<Void>() {
                
                @Override public Void call() {
                    try {
                        cookedTimeProcessorSemaphore.acquire();
                    
                        Session s=HibernateUtil.getSessionFactory().getCurrentSession();
                        s.beginTransaction();
                        logger.debug("Deleting all cooked times");

                        try {  
                            s.createQuery("delete from CookedTimeData").executeUpdate();
                        } catch (Exception e) {
                            logger.debug(e.getMessage());
                        } 
                        s.getTransaction().commit(); 

                        // Now go and clean up the cooked times observable list
                        Platform.runLater(() -> {
                            cookedTimeList.clear();
                        });

                        cookedTimeBibMap.clear();
                    } catch (InterruptedException ex) {
                        logger.warn("Interrupted!",ex);
                    }

                    cookedTimeProcessorSemaphore.release();
                    resultsDAO.reprocessAllResults();
                    return null;
                }
        };
        Thread clearAllCookedThread = new Thread(clearTimes);
        clearAllCookedThread.setName("Thread-clearAllCookedThread");
        clearAllCookedThread.setPriority(1);
        clearAllCookedThread.start();
                    
    }
    
    public void clearCookedTimes(TimingLocation tl) {
        tl.getInputs().stream().forEach(tli -> {
            clearCookedTimes(tli);
        });
    }
    
    void blockingClearCookedTimes(TimingLocationInput tli) {
        if (Platform.isFxApplicationThread()) logger.debug("clockingClearCookedTimes called on FxApplicationThread!!!");
        getCookedTimes(); // make sure the list exists
        
        final Integer tliID = tli.getID();
        final Predicate<CookedTimeData> fromThisInput = c -> Objects.equals(c.getTimingLocationInputId(), tliID);
        
        bulkOperations.incrementAndGet();
        resultsDAO.beginBusy();
        boolean locked = false;
        try {
            cookedTimeProcessorSemaphore.acquire();
            locked = true;
            
            // Anything still waiting to be saved for this input is no longer wanted
            cookedTimeQueue.removeIf(fromThisInput);
            
            // Ignored times are not in the map and do not feed into a result, so 
            // the bibs that come back are the only ones that need to be recalculated. 
            Set<String> bibs = removeFromBibMap(fromThisInput);

            Session s=HibernateUtil.getSessionFactory().getCurrentSession();
            s.beginTransaction();
            logger.debug("Deleting all cooked times for " + tli.getLocationName());

            try {  
                s.createQuery("delete from CookedTimeData where timingLocationInputId = :tli_id").setParameter("tli_id", tliID).executeUpdate();
            } catch (Exception e) {
                logger.debug(e.getMessage());
            } 
            s.getTransaction().commit(); 
            logger.debug("Done deleting all cooked times for " + tli.getLocationName());
            
            // The observable list is only safe to walk on the JavaFX thread
            Platform.runLater(() -> {
                FxBulk.removeIf(cookedTimeList, fromThisInput); 
            });
            
            resultsQueue.addAll(bibs);
        } catch (InterruptedException ex) {
            logger.warn("Interrupted!",ex);
        } finally {
            if (locked) cookedTimeProcessorSemaphore.release();
            bulkOperations.decrementAndGet();
            resultsDAO.endBusy();
        }
    }
    
    /**
     * Throw away the cooked times for a set of inputs and build them again from
     * the raw reads those inputs are holding.
     * 
     * This is done as one operation: one transaction, one swap of the times 
     * each bib has, one change to the observable list, and one request to 
     * recalculate the bibs involved. Doing it a time at a time (or even an
     * input at a time) makes the tables re-sort themselves over and over, 
     * which is what used to lock up the UI. 
     */
    public void reprocessReads(Collection<TimingLocationInput> inputs) {
        final List<TimingLocationInput> inputList = new ArrayList(inputs);
        if (inputList.isEmpty()) return;
        getCookedTimes(); // make sure the list and the processing thread exist
        
        bulkOperations.incrementAndGet();
        resultsDAO.beginBusy();
        
        Thread reprocessThread = new Thread(() -> {
            List<TimingLocationInput> paused = new ArrayList();
            boolean locked = false;
            try {
                // Hold off any new reads while we work
                for (TimingLocationInput tli: inputList) {
                    tli.pauseReads();
                    paused.add(tli);
                }
                cookedTimeProcessorSemaphore.acquire();
                locked = true;
                
                final Set<Integer> inputIDs = new HashSet();
                inputList.forEach(tli -> inputIDs.add(tli.getID()));
                final Predicate<CookedTimeData> fromTheseInputs = c -> inputIDs.contains(c.getTimingLocationInputId());
                
                // Pending saves for these inputs are about to be redone from the raw reads
                cookedTimeQueue.removeIf(fromTheseInputs);
                
                final List<CookedTimeData> newTimes = new ArrayList();
                for (TimingLocationInput tli: inputList) {
                    List<CookedTimeData> cooked = tli.cookAllReads();
                    logger.debug("TimingDAO::reprocessReads: " + tli.getLocationName() + " produced " + cooked.size() + " times");
                    newTimes.addAll(cooked);
                }
                
                Session s=HibernateUtil.getSessionFactory().getCurrentSession();
                s.beginTransaction();
                try {
                    s.createQuery("delete from CookedTimeData where timingLocationInputId in (:tli_ids)").setParameterList("tli_ids", inputIDs).executeUpdate();
                    int count = 0;
                    for (CookedTimeData c: newTimes) {
                        s.save(c);
                        if ( ++count % 100 == 0 ) {
                            //flush a batch of updates and release memory:
                            s.flush();
                            s.clear();
                        }
                    }
                    s.getTransaction().commit(); 
                } catch (Exception e) {
                    logger.error("TimingDAO::reprocessReads: Unable to save the reprocessed times", e);
                    try { s.getTransaction().rollback(); } catch (Exception e2) { logger.debug("Rollback failed", e2); }
                    return; // leave what we have in memory alone
                }
                
                // Swap the times for each bib
                Set<String> bibs = removeFromBibMap(fromTheseInputs);
                Map<String,List<CookedTimeData>> newByBib = new java.util.HashMap();
                newTimes.forEach(c -> newByBib.computeIfAbsent(c.getBib(), k -> new ArrayList()).add(c));
                newByBib.forEach((bib, times) -> cookedTimeBibMap.merge(bib, times, (a, b) -> {
                    List<CookedTimeData> both = new ArrayList(a);
                    both.addAll(b);
                    return both;
                }));
                bibs.addAll(newByBib.keySet());
                
                Platform.runLater(() -> {
                    FxBulk.replace(cookedTimeList, fromTheseInputs, newTimes);
                });
                
                resultsQueue.addAll(bibs);
                logger.debug("TimingDAO::reprocessReads: done. " + newTimes.size() + " times for " + bibs.size() + " bibs");
            } catch (InterruptedException ex) {
                logger.warn("Interrupted!",ex);
            } catch (Exception ex) {
                logger.error("TimingDAO::reprocessReads: exception", ex);
            } finally {
                if (locked) cookedTimeProcessorSemaphore.release();
                paused.forEach(tli -> tli.resumeReads());
                bulkOperations.decrementAndGet();
                resultsDAO.endBusy();
            }
        });
        reprocessThread.setName("Thread-reprocessReads");
        reprocessThread.setDaemon(true);
        reprocessThread.start();
    }
    
    void clearCookedTimes(TimingLocationInput tli) {
        // A reader will often clear its reads and then start right back up
        // again (re-reading a file). Hold off any new reads for this input
        // until the old times are gone so the cleanup can't take the new ones
        // out along with the old.
        //
        // If the input is busy right now (a read or a reprocess is in flight)
        // don't make the JavaFX thread wait for it, let the cleanup thread
        // do the waiting instead.
        final boolean pausedHere = tli.tryPauseReads();

        Task clearTimes = new Task<Void>() {

            @Override public Void call() {
                boolean paused = pausedHere;
                try {
                    if (!paused) {
                        tli.pauseReads();
                        paused = true;
                    }
                    // Go and clean up the cooked times observable list
                    blockingClearCookedTimes(tli);
                } catch (InterruptedException ex) {
                    logger.warn("Interrupted!",ex);
                } finally {
                    if (paused) tli.resumeReads();
                }
                return null;
            }
        };
        Thread clearCookedThread = new Thread(clearTimes);
        clearCookedThread.setPriority(1);
        clearCookedThread.start();
    }
    
    public Map<String,List<CookedTimeData>> getCookedTimesMap() {
        return cookedTimeBibMap;
    }
    
    public BlockingQueue<CookedTimeData> getCookedTimeQueue() {
        return cookedTimeQueue; 
    }
    
    public void addTimingLocation(TimingLocation tl) {
        Session s=HibernateUtil.getSessionFactory().getCurrentSession();
        s.beginTransaction();
        s.save(tl);
        s.getTransaction().commit();
        //Platform.runLater(() -> {
            timingLocationList.add(tl);
        //});
        
    }
    
    public void createDefaultTimingLocations() {
            
            blockingClearAll();
            
            Session s=HibernateUtil.getSessionFactory().getCurrentSession();
            s.beginTransaction();
            // sql to set the name and date
            Query query = s.createNativeQuery("INSERT into TIMING_LOCATION (TIMING_LOCATION_ID, TIMING_LOCATION_NAME) values (:id, :name)");
            query.setParameter("id", 1);
            query.setParameter("name", "Start");
            query.executeUpdate();
            query.setParameter("id", 2);
            query.setParameter("name", "Finish");
            query.executeUpdate();            
            s.getTransaction().commit();
            
            // Thread.dumpStack(); // who called this?
            refreshTimingLocationList();
        }
    
    private void refreshTimingLocationList() { 

        List<TimingLocation> list = new ArrayList<>();
        

        Session s=HibernateUtil.getSessionFactory().getCurrentSession();
        s.beginTransaction();
        logger.debug("Runing the refreshTimingLocationList Query");
        
        try {  
            list=s.createQuery("from TimingLocation order by id").list();
        } catch (Exception e) {
            logger.debug(e.getMessage(),e);
            
        } 
        s.getTransaction().commit(); 
        
        logger.debug("Returning the refreshTimingLocationList list");
        if(!timingLocationList.isEmpty())
            timingLocationList.clear();
        timingLocationList.addAll(list);
        locationsLoadedLatch.countDown();
    }     
    
    public ObservableList<TimingLocation> listTimingLocations() { 

        if (timingLocationList.isEmpty()) refreshTimingLocationList();
        return timingLocationList;
        //return list;
    }     
    
    public TimingLocation getTimingLocationByID(Integer id) {
        try {
            logger.trace("Looking for a timingLocation with id " + id);
            // This is ugly. Setup a map for faster lookups
            locationsLoadedLatch.await();
        } catch (InterruptedException ex) {
            logger.error("Interrupted!",ex);
        }
            
        Optional<TimingLocation> result = timingLocationList.stream()
                    .filter(t -> Objects.equals(t.getID(), id))
                    .findFirst();
        if (result.isPresent()) {
            logger.trace("Found " + result.get().LocationNameProperty());
            return result.get();
        } 
        
        return null;
    }

    public void removeTimingLocation(TimingLocation tl) {
        
        clearRawTimes(tl);
        Session s=HibernateUtil.getSessionFactory().getCurrentSession();
        s.beginTransaction();
        s.delete(tl);
        s.getTransaction().commit(); 
        timingLocationList.remove(tl);
    }      
    
    public void clearAllRawTimes() {
//        Session s=HibernateUtil.getSessionFactory().getCurrentSession();
//        s.beginTransaction();
//        logger.debug("Deleting all Raw times");
//        
//        try {  
//            s.createQuery("delete from RawTimeData").executeUpdate();
//        } catch (Exception e) {
//            logger.debug(e.getMessage());
//        } 
//        s.getTransaction().commit();
        
        //Clearing the raw times also clears the associated cooked times
        timingLocationList.forEach(t -> clearRawTimes(t));
        
    }
    
    public void clearAllTimes() {
        clearAllRawTimes();
        //clearAllCookedTimes();
    }
    
    public void reprocessAllRawTimes(){
        List<TimingLocationInput> inputs = new ArrayList();
        timingLocationList.forEach(t -> {
            logger.debug("TimingDAO::reprocessAllRawTimes: " + t.getLocationName());
            inputs.addAll(t.inputsProperty());
        });
        reprocessReads(inputs);
    }
    
//    public void clearAll() {
//        clearAllTimes(); 
//        removeTimingLocations(timingLocationList);
//    }
//    private void removeTimingLocations(ObservableList<TimingLocation> removeList) {
//        
//        Task task;
//        task = new Task<Void>() {
//            @Override public Void call() {
//                int max = removeList.size();
//                int i=1;
//                Session s=HibernateUtil.getSessionFactory().getCurrentSession();
//                s.beginTransaction();
//                int count = 0;
//                Iterator<TimingLocation> deleteMeIterator = removeList.iterator();
//                while (deleteMeIterator.hasNext()) {
//                    TimingLocation p = deleteMeIterator.next();
//                    
//                    s.delete(p); 
//                    if ( ++count % 20 == 0 ) {
//                        //flush a batch of updates and release memory:
//                        s.flush();
//                        s.clear();
//                    }
//                    updateProgress(i++, max);
//                }
//                s.getTransaction().commit(); 
//                
//                Platform.runLater(() -> {
//                    refreshTimingLocationList();
//                });
//                
//                return null;
//            }
//        };
//        new Thread(task).start();
//    }   
    public void blockingClearAll() {
        clearAllTimes(); 
        blockingRemoveTimingLocations(timingLocationList);
    }
    public void blockingRemoveTimingLocations(ObservableList<TimingLocation> removeList) {
        int max = removeList.size();
        int i=1;
        Session s=HibernateUtil.getSessionFactory().getCurrentSession();
        s.beginTransaction();
        int count = 0;
        Iterator<TimingLocation> deleteMeIterator = removeList.iterator();
        while (deleteMeIterator.hasNext()) {
            TimingLocation p = deleteMeIterator.next();
            
            s.delete(p); 
            if ( ++count % 20 == 0 ) {
                //flush a batch of updates and release memory:
                s.flush();
                s.clear();
            }

        }
        s.getTransaction().commit(); 

        //Platform.runLater(() -> {
                refreshTimingLocationList();
        //    });
                

    }  
    
    public void updateTimingLocation(TimingLocation tl) {
        Session s=HibernateUtil.getSessionFactory().getCurrentSession();
        s.beginTransaction(); 
        s.update(tl);
        s.getTransaction().commit();
     } 
    
    public void addTimingLocationInput(TimingLocationInput t) {
        if (t.getTimingLocation() != null) t.getTimingLocation().addInput(t);
        Session s=HibernateUtil.getSessionFactory().getCurrentSession();
        s.beginTransaction();
        s.save(t);
        s.getTransaction().commit();
      
    }
    public void updateTimingLocationInput(TimingLocationInput t) {
        
        Session s=HibernateUtil.getSessionFactory().getCurrentSession();
        s.beginTransaction();
        s.saveOrUpdate(t);
        //s.update(t);
        s.getTransaction().commit();
      
    }
    public void removeTimingLocationInput(TimingLocationInput t) {
        // clear out any raw times
        clearRawTimes(t); 
        // clear out any cooked times
        clearCookedTimes(t); 
        // unlink it from the timing location
        if (t.getTimingLocation() != null) t.getTimingLocation().removeInput(t);
        // get it out of the db
        Session s=HibernateUtil.getSessionFactory().getCurrentSession();
        s.beginTransaction();
        s.delete(t);
        s.getTransaction().commit();
      
    }
    
//    public void timingLocationInputPauseReader(TimingLocationInput tl) {
//        //Set a flag so that the 
//    }
//    public void timingLocationInputStartReader(TimingLocationInput tl) {
//        
//        
//    }   
    
    public String getBibFromChip(String c) {
        if (bib2ChipMap == null) {
            bib2ChipMap = getBib2ChipMap();
        }
        return bib2ChipMap.getBibFromChip(c);
    }
    
    public Bib2ChipMap getBib2ChipMap() {
        if (bib2ChipMap != null) return bib2ChipMap;
        
        logger.debug("Runing the getBib2ChipMap querry");
        List<Bib2ChipMap> mapList = HibernateUtil.loadReadOnly("from Bib2ChipMap");
         if(mapList.size()>0) {
             bib2ChipMap = mapList.get(0);
         } else {
             bib2ChipMap = new Bib2ChipMap();
         }
         return bib2ChipMap;
    }
    
    public void saveBib2ChipMap(Bib2ChipMap b) {
        Session s=HibernateUtil.getSessionFactory().getCurrentSession();
        s.beginTransaction();
        s.saveOrUpdate(b);
        s.getTransaction().commit();
    }
    public void updateBib2ChipMap(Bib2ChipMap b) {
        Session s=HibernateUtil.getSessionFactory().getCurrentSession();
        s.beginTransaction();
        s.update(b);
        s.getTransaction().commit();
    }
    
    
    
    
    
    public ObservableList<TimeOverride> getOverrides() {
        
        // if the cooked time list is null, then let's create one,
        // fetch fetch existing datapoints from the DB,
        // and add them to the cookedTimesList;
        
        if (overrideList == null) {
            overrideList =FXCollections.observableArrayList(TimeOverride.extractor());
            
            Task fetchOverrideFromDB = new Task<Void>() {
                
                @Override public Void call() {
                   logger.debug("Runing the getOverrides querry");
                   final List<TimeOverride> dbOverrides = HibernateUtil.loadReadOnly("from TimeOverride");
                    
                    Platform.runLater(() -> {
                        overrideList.addAll(dbOverrides);
                    });
                    dbOverrides.stream().forEach(c -> {
                        if (!overrideMap.containsKey(c.getBib())) overrideMap.put(c.getBib(), new ArrayList());
                        overrideMap.get(c.getBib()).add(c);
                    });
                    
                    return null; 
                }
            }; 
            new Thread(fetchOverrideFromDB).start();
            
            
        }
    
        return overrideList;
        
        
    }
    
    public Optional<List<TimeOverride>> getOverridesByBib(String bib) {
        if(overrideMap.containsKey(bib) && ! overrideMap.get(bib).isEmpty() ){
            return Optional.of(overrideMap.get(bib));
        } else return Optional.empty();
        
    }
    
    public void saveOverride(TimeOverride o) {
        Session s=HibernateUtil.getSessionFactory().getCurrentSession();
        s.beginTransaction();
        s.saveOrUpdate(o);
        s.getTransaction().commit();
        
        // Just in case.... This will make sure we initialized the overrideList
        getOverrides();
        
        
        
        if (!overrideMap.containsKey(o.getBib())) overrideMap.put(o.getBib(), new ArrayList()); 
        
        
        // If this is an edit, don't add it again
        if (!overrideMap.get(o.getBib()).contains(o)) {
            // if this is an overwrite, remove the old one first
            try {
                TimeOverride old = overrideMap.get(o.getBib()).stream().filter(e -> e.getSplitId().equals(o.getSplitId())).findFirst().get();
                deleteOverride(old);
            }
            catch (Exception e) {

            }
            overrideMap.get(o.getBib()).add(o);
        }
        
        if (!overrideList.contains(o)) Platform.runLater(() -> {
             overrideList.add(o);
        });
        
        // Check to see if this one replaces another
        
        
        resultsQueue.add(o.getBib());
    }
    
    public void reprocessBib(String bib){
        resultsQueue.add(bib);
    }
    
    public void deleteOverride(TimeOverride o) {
        // Just in case.... This will make sure we initialized the overrideList
        getOverrides();
        
        Platform.runLater(() -> {
            overrideList.remove(o);
        });
        overrideMap.get(o.getBib()).remove(o);
        Session s=HibernateUtil.getSessionFactory().getCurrentSession();
        s.beginTransaction();
        s.delete(o);
        s.getTransaction().commit();
        resultsQueue.add(o.getBib());
    }
    
    public void clearAllOverrides(){
        Session s=HibernateUtil.getSessionFactory().getCurrentSession();
        s.beginTransaction();
        logger.debug("Deleting all overrides...");
        overrideList.forEach(o -> resultsQueue.add(o.getBib()));
        try {  
            s.createQuery("delete from TimeOverride").executeUpdate();
            Platform.runLater(() -> {
                overrideList.clear();
            });
            overrideMap = new ConcurrentHashMap();
        } catch (Exception e) {
            logger.debug(e.getMessage());
        } 
        s.getTransaction().commit();
    }
}
