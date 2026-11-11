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

import org.pikatimer.race.Race;
import org.pikatimer.util.StringCapitalizationNormalizer;
import java.util.Map;
import jakarta.persistence.CollectionTable;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.MapKeyColumn;
import jakarta.persistence.Table;
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

    ////////////////////////////////////////////////////////////////////////
    // OAuth2 (PKCE) client configuration
    // TODO: Update these once the values have been issued by RunSignUp
    ////////////////////////////////////////////////////////////////////////
    public static String OAUTH_CLIENT_ID = "";
    // RSU's spec lists client_secret as required on the token endpoints.
    // If left blank it will be omitted from the token requests.
    public static String OAUTH_CLIENT_SECRET = "";
    // Must exactly match the redirect URI registered with RSU.
    // The local callback listener binds to the host/port/path in this URI.
    public static String OAUTH_REDIRECT_URI = "http://127.0.0.1:53682/rsu/oauth/callback";

    public static final String OAUTH_AUTHORIZE_URL = "https://api.runsignup.com/Profile/OAuth2/RequestGrant";
    public static final String OAUTH_TOKEN_URL = "https://api.runsignup.com/rest/v2/auth/auth-code-redemption.json";
    public static final String OAUTH_REFRESH_URL = "https://api.runsignup.com/rest/v2/auth/refresh-token.json";
    public static final String OAUTH_SCOPE = "rsu_api_read rsu_api_write";

    // Login Types (persisted in the rsuLoginType column)
    public static final String LOGIN_API_V1 = "API";        // legacy api_key / api_secret
    public static final String LOGIN_API_V2 = "API_V2";     // rsu_api_key + X-RSU-API-SECRET header
    public static final String LOGIN_OAUTH = "OAUTH";       // OAuth2 Bearer token
    public static final String LOGIN_PASSWORD = "PASSWORD"; // legacy username/password -> tmp_key/tmp_secret

    Integer id;
    String rsuKey;
    String rsuSecret;
    String rsuLoginType;
    
    String rsuTempKey;
    String rsuTempSecret;
    Long rsuTempTimestamp=0L;

    // OAuth2 tokens
    String rsuAccessToken;
    String rsuRefreshToken;
    Long rsuTokenExpires = 0L;
    String rsuOAuthUser;

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
        return rsuKey; 
    }
    public void setRSUUsername(String  rsuUsername) {
        this.rsuKey = rsuUsername;
        //logger.debug("Participant UUID is now " + uuidProperty.get());
    }
    
    @Column(name="rsuPassword")
    public String getRSUPassword() {
       // logger.debug("Participant UUID is " + uuidProperty.get());
        return rsuSecret; 
    }
    public void setRSUPassword(String  rsuPassword) {
        this.rsuSecret = rsuPassword;
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
    
    @Column(name="rsuAccessToken")
    public String getRSUAccessToken() {
        return rsuAccessToken;
    }
    public void setRSUAccessToken(String t) {
        this.rsuAccessToken = t;
    }

    @Column(name="rsuRefreshToken")
    public String getRSURefreshToken() {
        return rsuRefreshToken;
    }
    public void setRSURefreshToken(String t) {
        this.rsuRefreshToken = t;
    }

    @Column(name="rsuTokenExpires")
    public Long getRSUTokenExpires() {
        return rsuTokenExpires;
    }
    public void setRSUTokenExpires(Long ts) {
        this.rsuTokenExpires = (ts == null) ? 0L : ts;
    }

    @Column(name="rsuOAuthUser")
    public String getRSUOAuthUser() {
        return rsuOAuthUser;
    }
    public void setRSUOAuthUser(String u) {
        this.rsuOAuthUser = u;
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
        return raceToEventMap.get(race.getID());
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
    public Map<Integer,Integer> getRaceToRSUEventMap(){
        return raceToEventMap;
    }
    
    public void setRaceToRSUEventMap(Map<Integer,Integer> eventMap) {
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
