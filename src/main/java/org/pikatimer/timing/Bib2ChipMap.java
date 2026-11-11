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

import java.util.HashMap;
import java.util.Map;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.IntegerProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleIntegerProperty;
import jakarta.persistence.CollectionTable;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.MapKeyColumn;
import jakarta.persistence.Table;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.GenericGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jcgarner
 */
@Entity
@DynamicUpdate
@Table(name="bib2chip")
public class Bib2ChipMap {
    private static final Logger logger = LoggerFactory.getLogger(Bib2ChipMap.class);
    
    private final IntegerProperty IDProperty = new SimpleIntegerProperty(); // bib2chip_id
    private Map<String,String> chip2bibMap;
    private final BooleanProperty customMapProperty = new SimpleBooleanProperty(false);
    
    public Bib2ChipMap() {
        chip2bibMap=new HashMap<>();
    }
    
    @Id
    @GenericGenerator(name="bib2chip_id" , strategy="increment")
    @GeneratedValue(generator="bib2chip_id")
    @Column(name="bib2chip_id")
    public Integer getID() {
        return IDProperty.getValue(); 
    }
    public void setID(Integer id) {
        IDProperty.setValue(id);
    }
    public IntegerProperty idProperty() {
        return IDProperty; 
    }
    
    @Column(name="custom_map")
    public Boolean getUseCustomMap() {
        return customMapProperty.getValue(); 
    }
    public void setUseCustomMap(Boolean id) {
        customMapProperty.setValue(id);
    }
    public BooleanProperty useCustomMapProperty() {
        return customMapProperty; 
    }
    
    // Where we stash the attributes for the input
    @ElementCollection(fetch = FetchType.EAGER)
    @MapKeyColumn(name="chip")
    @Column(name="bib")
    @CollectionTable(name="bib2chipmap", joinColumns=@JoinColumn(name="bib2chip_id"))
    public Map<String, String> getChip2BibMap() {
        return chip2bibMap;
    }
    public void setChip2BibMap(Map<String,String> chip2bib) {
        
        
        if (chip2bib != null) {
            chip2bibMap = chip2bib;
        } else {
            chip2bibMap = new HashMap();
        }
    } 
    
    public String getBibFromChip(String Chip) {

        if(customMapProperty.getValue() && chip2bibMap.size() > 0 ) {

            if (chip2bibMap.containsKey(Chip)) return chip2bibMap.get(Chip);
            else if ("0".equals(Chip)) return Chip; // special chip of "0" 
            return "Unmapped " + Chip;
        } else {

            return Chip;
        }
    }
}
