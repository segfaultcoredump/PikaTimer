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

import com.pikatimer.race.Race;
import com.pikatimer.util.StringCapitalizationNormalizer;
import java.util.Map;
import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.MapKeyColumn;
import javax.persistence.Table;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.GenericGenerator;

/**
 *
 * @author John Garner <segfaultcoredump@gmail.com>
 */
@Entity
@DynamicUpdate
@Table(name="RSUConfig")
public class RSUConfig {
    Integer id;
    String rsuUsername;
    String rsuPassword;
    String rsuLoginType;
    
    String rsuTempKey;
    String rsuTempSecret;
    Long rsuTempTimestamp=0L;
    
    Integer rsuRaceID;
    
    Long rsuLastSync = 0L;
    
    Boolean bidiSync = true;
    StringCapitalizationNormalizer capNormalize = StringCapitalizationNormalizer.TitleCase;
    
    Map<Integer,Integer> eventToRaceMap;
    
    Map<Integer,Integer> raceToEventMap;
    
    
    public RSUConfig(){
        
    }
    
    @Id
    @GenericGenerator(name="rsuconfig_id" , strategy="increment")
    @GeneratedValue(generator="rsuconfig_id")
    @Column(name="id")
    public Integer getID() {
        return id;
    }
    public void setID(Integer id) {
        this.id = id;
    }
    
    @Column(name="rsuUsername")
    public String getRSUUsername() {
       // logger.debug("Participant UUID is " + uuidProperty.get());
        return rsuUsername; 
    }
    public void setRSUUsername(String  rsuUsername) {
        this.rsuUsername = rsuUsername;
        //logger.debug("Participant UUID is now " + uuidProperty.get());
    }
    
    @Column(name="rsuPassword")
    public String getRSUPassword() {
       // logger.debug("Participant UUID is " + uuidProperty.get());
        return rsuPassword; 
    }
    public void setRSUPassword(String  rsuPassword) {
        this.rsuPassword = rsuPassword;
        //logger.debug("Participant UUID is now " + uuidProperty.get());
    }
    
    @Column(name="rsuLoginType")
    public String getRSULoginType() {
       // logger.debug("Participant UUID is " + uuidProperty.get());
        return rsuLoginType; 
    }
    public void setRSULoginType(String  rsuLoginType) {
        this.rsuLoginType = rsuLoginType;
        //logger.debug("Participant UUID is now " + uuidProperty.get());
    }
    
    @Column(name="rsuRaceID")
    public Integer getRSURaceID() {
       // logger.debug("Participant UUID is " + uuidProperty.get());
        return rsuRaceID; 
    }
    public void setRSURaceID(Integer  rsuRaceID) {
        this.rsuRaceID = rsuRaceID;
        //logger.debug("Participant UUID is now " + uuidProperty.get());
    }
    
    @Column(name="rsulastsync")
    public Long getRSULastSync() {
       // logger.debug("Participant UUID is " + uuidProperty.get());
        return rsuLastSync; 
    }
    public void setRSULastSync(Long  ts) {
        this.rsuLastSync = ts;
        //logger.debug("Participant UUID is now " + uuidProperty.get());
    }
    
    public Integer getRSUEvent(Race race){
        return eventToRaceMap.entrySet().stream()
            .filter(e -> e.getValue().equals(race.getID()))
            .map(Map.Entry::getKey)
            .findFirst()
            .orElse(null);
    }
    
    @ElementCollection(fetch = FetchType.EAGER)
    @MapKeyColumn(name="raceID")
    @Column(name="rsuEventID")
    @CollectionTable(name="rsuconfig_eventmap", joinColumns=@JoinColumn(name="configID"))
    public Map<Integer,Integer> getRSUEventMap(){
        return eventToRaceMap;
    }
    
    public void setRSUEventMap(Map<Integer,Integer> eventMap) {
        eventToRaceMap = eventMap;
    }
    
    @ElementCollection(fetch = FetchType.EAGER)
    @MapKeyColumn(name="rsuEventID")
    @Column(name="raceID")
    @CollectionTable(name="rsuconfig_default_eventmap", joinColumns=@JoinColumn(name="configID"))
    public Map<Integer,Integer> getRSUDefaultEventMap(){
        return raceToEventMap;
    }
    
    public void setRSUDefaultEventMap(Map<Integer,Integer> eventMap) {
        raceToEventMap = eventMap;
    }
    
    
    @Column(name="bidirectional") // boolean
    public Boolean getBiDiSync(){
        return bidiSync;
    }
    public void setBiDiSync(Boolean b){
        bidiSync = b;
    }
    
    @Enumerated(EnumType.STRING)
    @Column(name="capitalization")
    public StringCapitalizationNormalizer getNormalizeCapitalization() {
        return capNormalize;
    }
    public void setNormalizeCapitalization(StringCapitalizationNormalizer n){
        capNormalize = n;
    }
    
    
}
