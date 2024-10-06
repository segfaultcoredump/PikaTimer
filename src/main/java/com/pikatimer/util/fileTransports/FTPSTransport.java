/*
 * Copyright (C) 2016 jcgarner
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
package com.pikatimer.util.fileTransports;

import com.pikatimer.results.ReportDestination;
import com.pikatimer.util.DurationFormatter;
import com.pikatimer.util.FileTransport;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import javafx.concurrent.Task;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.net.ftp.FTPSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jcgarner
 */
public class FTPSTransport implements FileTransport{
    private static final Logger logger = LoggerFactory.getLogger(FTPSTransport.class);
    
    String basePath;
    ReportDestination parent;
    Boolean stripAccents = false;

    Thread transferThread;
    FTPSClient ftpsClient;
    FTPClient ftpClient;
    
    String protMode="P";
    
    private static final BlockingQueue<String> transferQueue = new ArrayBlockingQueue(100000);

    private static final Map<String,String> transferMap = new ConcurrentHashMap();
    
    StringProperty transferStatus = new SimpleStringProperty("Idle");
    
    String hostname;
    String username;
    String password;

    Boolean fatalError = false;
    Boolean needConfigRefresh = true;
    Boolean encrypted = true;
    Long lastTransferTimestamp = 0L;

    public FTPSTransport() {
        
        Task transferTask = new Task<Void>() {

                @Override 
                public Void call() {
                   

                    logger.debug("FTPSTransport: new result processing thread started");
                    String filename = null;
                    while(true) {
                        try {
                            logger.debug("FTPSTransport Thread: Waiting for the first file...");
                            if (filename == null) filename = transferQueue.take();
                            
                            while(true) {
                                logger.debug("FTPSTransport Thread: Waiting for a file...");
                                //filename = transferQueue.poll(60, TimeUnit.SECONDS);
                                if (ftpClient == null || !ftpClient.isConnected()) Platform.runLater(() -> {transferStatus.set("Idle");});
                                else {
                                    ftpClient.sendNoOp();
                                    Platform.runLater(() -> {
                                        if (encrypted) transferStatus.set("Connected FTPS");
                                        else transferStatus.set("Connected");
                                    });
                                }
                                
                                //filename = transferQueue.take(); // blocks until
                                if (filename == null) filename = transferQueue.poll(15, TimeUnit.SECONDS);
                                if (filename == null) {
                                    // If we have been idle for more than 2 minutes, be nice and drop the connection
                                    if (TimeUnit.NANOSECONDS.toSeconds((System.nanoTime()-lastTransferTimestamp))> 120 ) break;
                                    else continue;
                                }

                                logger.debug("FTPSTransport Thread: Transfering " + filename);
                                String contents = transferMap.get(filename);
                                
                                while (fatalError || ftpClient == null || !ftpClient.isConnected()) {
                                    if (!fatalError) openConnection();
                                    if (!ftpClient.isConnected()) {
                                        logger.debug("FTPSTransport Thread: Still not connected, sleeping for 10 seconds...");
                                        Thread.sleep(10000);
                                    }
                                    
                                }

                                InputStream data = IOUtils.toInputStream(contents, "UTF-8");
                                //InputStream data = IOUtils.toInputStream(contents);
                                String fn = filename;
                                String tmpFn = fn + ".PikaTmp";
                                Platform.runLater(() -> { 
                                    if (encrypted && protMode.equalsIgnoreCase("P")) transferStatus.set("Transfering (Secure) " + fn);
                                    else if (encrypted ) transferStatus.set("Transfering (Clear) " + fn);
                                    else transferStatus.set("Transfering " + fn);
                                });
                                long startTime = System.nanoTime();
                                
                                // To get around file locking issues, 
                                // we upload to a temp fie and then do a rename
                                
                                ftpClient.storeFile(tmpFn, data);
                                
                                // Try a rename. If it fails, try a delete and then a rename
                                try {  
                                    ftpClient.rename(tmpFn, fn);
                                } catch (Exception ex){
                                    logger.debug("ftpClient.rename exception thrown");
                                    
                                    try { 
                                        ftpClient.dele(fn);// This may fail if the file does not exist
                                    } catch (Exception ex2){
                                        // noop
                                        logger.debug("ftpClient.dele exception thrown");
                                    }
                                    ftpClient.rename(tmpFn, fn);
                                }
                                
                                long endTime = System.nanoTime();
                                lastTransferTimestamp = endTime;

                                data.close();
                                transferMap.remove(filename, contents); 
                                
                                logger.debug("FTPSTransport Thread: transfer of " + filename + " done in " + DurationFormatter.durationToString(Duration.ofNanos(endTime-startTime), 3, false, RoundingMode.HALF_EVEN));
                                filename = null;
                            }

                        } catch (InterruptedException ex) {
                            logger.debug("FTPSTransport Thread: InterruptedException thrown");
                            //if (filename!= null) transferQueue.put(filename);

                            //Logger.getLogger(FTPSTransport.class.getName()).log(Level.SEVERE, null, ex);
                        } catch (IOException ex) {
                            logger.debug("FTPSTransport Thread: IOException thrown: "  + ex.getMessage() );
                            
                            if (encrypted) {
                                logger.debug("Setting protection mode to \"C\" to see if that fixes the problem");
                                protMode="C";
                            }
                            
                            ex.printStackTrace();
                            //if (filename!= null) transferQueue.put(filename);
                            //Logger.getLogger(FTPSTransport.class.getName()).log(Level.SEVERE, null, ex);
                        } catch (Exception ex) {
                            logger.debug("FTPSTransport Thread: Exception tossed: " + ex.getMessage() + ex.toString());
                            logger.debug(Arrays.toString(ex.getStackTrace()));
                        } finally {
                            if (ftpClient.isConnected()) {
                                try {
                                    logger.debug("FTPSTransport Thread: calling ftpClient.disconnect()");
                                    ftpClient.disconnect();
                                    Platform.runLater(() -> {transferStatus.set("Disconnected");});
                                } catch (IOException f) {
                                    // do nothing
                                }
                            }
                        }
                    }
                    
                }
            };
            transferThread = new Thread(transferTask);
            transferThread.setName("Thread-FTPS-Transfer");
            transferThread.setDaemon(true);
            transferThread.start();
        
    }
    
    private void openConnection(){
        try {
            
            if (needConfigRefresh) refreshConfig();
            logger.debug("FTPS Not connected, connecting...");
            Platform.runLater(() -> {
                if (encrypted) transferStatus.set("Connecting FTPS...");
                else transferStatus.set("Connecting...");
            });
            // connect to the remote server
            
            if (encrypted) {
                ftpsClient = new FTPSClient(false);
                ftpClient = ftpsClient;
            } else {
                ftpClient = new FTPClient();
                ftpsClient = null;
            }
            
            ftpClient.addProtocolCommandListener(new PrintCommandListener(new PrintWriter(System.out), true));
            // Connect to host
            ftpClient.setConnectTimeout(10000); // 10 seconds
            ftpClient.connect(hostname);
            int reply = ftpClient.getReplyCode();
            if (FTPReply.isPositiveCompletion(reply)) {
                
                
                // Login
                Platform.runLater(() -> {
                    if (encrypted) transferStatus.set("Loging in...");
                    else transferStatus.set("Loging in (insecure)...");
                });
                
                

                if (ftpClient.login(username, password)) {

                    if (encrypted) {
                        // Set protection buffer size
                        ftpsClient.execPBSZ(0);
                        // Set data channel protection mode ("P" or "C")
                      
                        ftpsClient.execPROT(protMode);
                    }
                    // Enter local passive mode
                    ftpClient.enterLocalPassiveMode();
                    //ftpClient.setFileType(FTP.ASCII_FILE_TYPE);
                    ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
                    //ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
                    
                    Platform.runLater(() -> {transferStatus.set("Changing Directories...");});
                    if(!ftpClient.changeWorkingDirectory(basePath)) {
                        reply = ftpClient.mkd(basePath);
                        
                        if (!FTPReply.isPositiveCompletion(reply)) {
                            logger.debug("Unable to make remote dir " + basePath);
                            Platform.runLater(() -> {transferStatus.set("Error: Unabe to make target directory");});
                            fatalError=true;
                            ftpClient.disconnect();
                        } else {
                            ftpClient.changeWorkingDirectory(basePath);
                            fatalError=false;
                        }
                    }
                    Platform.runLater(() -> {transferStatus.set("Connected");});
                    fatalError=false;
                } else {
                  logger.debug("FTP login failed");
                  Platform.runLater(() -> {transferStatus.set("Error: Login Failed");});
                  fatalError=true;
                  try {ftpClient.disconnect();} catch (Exception e) {};
                }
            } else {
              logger.debug("FTP connect to host failed");
              Platform.runLater(() -> {transferStatus.set("Error: Unable to connect to host");});

              try {ftpClient.disconnect();} catch (Exception e) {};
            }
        } catch (IOException ioe) {
            if (encrypted) {
                try {ftpClient.disconnect();} catch (Exception e) {};
                
                // odds are we don't support encryption, 
                // let's disable the encryption and try again
                encrypted = false;
                openConnection();
            } else {
                logger.debug("FTP client received network error");
                Platform.runLater(() -> {transferStatus.set("Error: Network Error");});
            }
            

        }
    }

    @Override
    public StringProperty statusProperty() {
        return transferStatus;
    }
     @Override
    public boolean isOK() {
        if (password.isEmpty() || username.isEmpty() || hostname.isEmpty() || basePath.isEmpty()) return false;
        return true;
    }

    @Override
    public void save(String filename, String contents) {
        logger.debug("FTPSTransport.save() called for " + filename);
        if (stripAccents) contents = StringUtils.stripAccents(contents);
        transferMap.put(filename,contents);
        if (! transferQueue.contains(filename)) transferQueue.add(filename);
        
        //if (transferThread == null || ftpClient == null || !ftpClient.isConnected() ) transferFile(); // kicks off the thread
    }

    @Override
    public void setOutputPortal(ReportDestination op) {
        parent=op;
    }

    @Override
    public void refreshConfig() {
        try {
        // Get the hostname, username, password, basePath
        password=parent.getPassword();
        username=parent.getUsername();
        hostname=parent.getServer();
        basePath=parent.getBasePath();
        if (ftpClient != null && ftpClient.isConnected()) {
            try {
                logger.debug("FTPSTransport::refreshConfig: calling ftpClient.disconnect()");
                ftpClient.disconnect();
            } catch (IOException f) {
                // do nothing
            }
        }
        
        stripAccents = parent.getStripAccents();
        
        encrypted = true;
                    
        fatalError=false;
        needConfigRefresh = false;
        } catch (Exception e){
            logger.debug("FTPSTransport::refreshConfig Exception!");
            e.printStackTrace();
            fatalError=true;
            needConfigRefresh = true;
        }
    }    

    @Override
    public void test(ReportDestination parent, StringProperty output) {
        Task transferTask = new Task<Void>() {

                @Override 
                public Void call() {
                    
                    password=parent.getPassword();
                    username=parent.getUsername();
                    hostname=parent.getServer();
                    basePath=parent.getBasePath();

                    Platform.runLater(() -> output.set(output.getValueSafe() + "Connecting to " + hostname +" via FTPS..." ));
                    
                    
                    ftpsClient = new FTPSClient(false);
                    ftpClient = ftpsClient;
                    ftpClient.addProtocolCommandListener(new PrintCommandListener(new PrintWriter(System.out), true));
                    

                    // Connect to host
                    
                    try {
                        ftpClient.connect(hostname);
                        Platform.runLater(() -> output.set(output.getValueSafe() + "\nConnected via via FTPS..." ));
                    } catch (IOException ex) {
                        Platform.runLater(() -> output.set(output.getValueSafe() + "\nConnection Failed, attempting to use FTP..." ));
                        ftpClient = new FTPClient();
                        ftpClient.setConnectTimeout(10000); // 10 seconds
                        ftpClient.addProtocolCommandListener(new PrintCommandListener(new PrintWriter(System.out), true));
                        try {
                            ftpClient.connect(hostname);
                            Platform.runLater(() -> output.set(output.getValueSafe() + "\nConnected via via FTP..." ));

                        } catch (IOException ex1) {
                            Platform.runLater(() -> output.set(output.getValueSafe() + "\n\nError: " + ex1.getLocalizedMessage() ));
                            try {
                                ftpClient.disconnect();
                            } catch (IOException ex2) {
                            }
                            return null;
                        }
                    }
                    
                    int reply = ftpClient.getReplyCode();
                    
                    if (FTPReply.isPositiveCompletion(reply)) {
                        try {
                            Platform.runLater(() -> output.set(output.getValueSafe() + "\nLogging in..." ));
                            
                            ftpClient.enterLocalPassiveMode(); 

                            if (ftpClient.login(username, password)) {
                                // try and CD to the target directory
                                Platform.runLater(() -> output.set(output.getValueSafe() + "\nLogin successful." ));

                                Platform.runLater(() -> output.set(output.getValueSafe() + "\nChanging directories..." ));

                                if(!ftpClient.changeWorkingDirectory(basePath)) {
                                    Platform.runLater(() -> output.set(output.getValueSafe() + "\nTarget directory does not exist\n Attempting to create it..." ));

                                    int reply2 = ftpClient.mkd(basePath);
                                    

                                    if (!FTPReply.isPositiveCompletion(reply2)) {
                                        Platform.runLater(() -> output.set(output.getValueSafe() + "\nError: Unable to make the target directory.\n\nTest Failed!" ));
                                        ftpClient.disconnect();
                                        return null;
                                    } else {
                                        
                                        if (!ftpClient.changeWorkingDirectory(basePath)) {
                                            Platform.runLater(() -> output.set(output.getValueSafe() + "\nError: Unabe to change to target directory\n\nTest Failed!" ));
                                            ftpClient.disconnect();
                                            return null;
                                        } else {
                                            Platform.runLater(() -> output.set(output.getValueSafe() + "\n\nSuccess!"));
                                            ftpClient.disconnect();
                                            return null;
                                        }
                                        
                                    }
                                } else {
                                    Platform.runLater(() -> output.set(output.getValueSafe() + "\n\nSuccess!"));
                                    ftpClient.disconnect();
                                    return null;
                                }
                            } else {
                                Platform.runLater(() -> output.set(output.getValueSafe() + "\nError: Invalid Username or Password\n\nTest Failed!" ));
                                ftpClient.disconnect();
                                return null;
                            }
                        } catch (IOException ex) {
                            Platform.runLater(() -> output.set(output.getValueSafe() + "\nError: IO Error " + ex.getLocalizedMessage()+ "\n\nTest Failed!"  ));
                            try {
                                ftpClient.disconnect();
                            } catch (IOException ex1) {
                                logger.warn("IO exception when disconnecting",ex);
                            }
                        }
                    } else {
                        Platform.runLater(() -> output.set(output.getValueSafe() + "\n\nError: Error code response from server: " + reply ));
                        try {
                            ftpClient.disconnect();
                        } catch (IOException ex) {
                            logger.warn("IO exception when disconnecting",ex);
                        }
                        return null;
                    }
                    try {
                        ftpClient.disconnect();
                    } catch (IOException ex) {
                        logger.warn("IO exception when disconnecting",ex);
                    }
                    return null;
                }
        
        };
        transferThread = new Thread(transferTask);
        transferThread.setName("Thread-SFTP-Transfer-Test");
        transferThread.setDaemon(true);
        transferThread.start();
    }
}
