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
package com.pikatimer.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author John Garner <segfaultcoredump@gmail.com>
 */
public enum StringCapitalizationNormalizer {
    NONE, // NOOP
    TitleCase, // Titlecase Everyting
    lowercase, // lowercase everything
    UPPERCASE; // UPPERCASE EVERYTHING
    
    private static final Logger logger = LoggerFactory.getLogger(StringCapitalizationNormalizer.class);

    
    private static final Map<StringCapitalizationNormalizer, String> CASE_MAP = createMap();

    private static Map<StringCapitalizationNormalizer, String> createMap() {
        Map<StringCapitalizationNormalizer, String> result = new HashMap<>();
        result.put(TitleCase, "Title Case");
        result.put(UPPERCASE,"UPPERCASE");
        result.put(lowercase, "lowercase");
        result.put(NONE, "None: leave as entered");
        return Collections.unmodifiableMap(result);
    }

    
    @Override 
    public String toString(){
        return CASE_MAP.get(this);
    }

    public String normalize(String s){
        switch(this){
            case NONE -> {
                return s;
            }
            case TitleCase -> {
                return titleCase(s);
            }
            case lowercase -> {
                return s.toLowerCase();
            } 
            case UPPERCASE -> {
                return s.toUpperCase();
            }

        }
        return s;
    }
    public static String titleCase(String s){
        if (s == null || s.isEmpty()) return "";
        
        if (s.equals(s.toLowerCase()) || s.equals(s.toUpperCase())) {
            String n = capitalizeSentence(s);
            n= n.replace(" Iii"," III").replace(" Ii", " II");
            if (n.length() == 2) n = upperIfNoVowel(n);
            if (! s.equals(n)) logger.debug("titleCase() Changed: " + s + " -> " + n);
            return n;
        }
        return s;
    }
    
    // This is borrowed from https://stackoverflow.com/questions/32249723/how-to-capitalize-first-letter-after-period-in-each-sentence-using-java
    private static String capitalizeSentence(String sentence) {
        StringBuilder result = new StringBuilder();
        boolean capitalize = true; //state
        for(char c : sentence.toCharArray()) {    
            if (capitalize) {
               //this is the capitalize state
               result.append(Character.toUpperCase(c));
               if (!Character.isWhitespace(c) && c != '.' && c != '\'' && c !='-') {
                 capitalize = false; //change state
               }
            } else {
               //this is the don't capitalize state
               result.append(Character.toLowerCase(c));
               if (c == '.' || Character.isWhitespace(c) || c == '\'' && c =='-') {
                 capitalize = true; //change state
               }
            }
        }
        return result.toString();
    }

    private static String upperIfNoVowel(String n) {
        // if both letters are constanants (e.g, "JT" or "MC"), 
        // then uppercase them
        if (n.length() != 2) return n;
        char[] a = n.toCharArray();
        if (a[0] != 'a' && a[0] != 'e' && a[0] != 'i' && a[0] != 'o' && a[0] != 'u' && 
            a[1] != 'a' && a[1] != 'e' && a[1] != 'i' && a[1] != 'o' && a[1] != 'u' ){
            return n.toUpperCase();
        }
                
        return n;
    }
}
