/*
 * Copyright (C) 2026 John Garner <segfaultcoredump@gmail.com>
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
package org.pikatimer.util;

import java.time.Duration;
import java.util.function.Function;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.scene.control.TextField;

/**
 * A time-of-day or duration field set up by {@link TextFieldFormatters}. The
 * {@link TimePattern} can be swapped at runtime, e.g. when a "relative to the
 * start" checkbox changes what the field means.
 *
 * @param <T> LocalTime or Duration
 * @author John Garner <segfaultcoredump@gmail.com>
 */
public class TemporalField<T> extends ValidatedField<T> {

    private final ObjectProperty<TimePattern> pattern;
    private final boolean timeOfDay;
    private String autoPrompt;

    TemporalField(TextField textField, TimePattern initial, Function<Duration, T> convert) {
        this(textField, new SimpleObjectProperty<>(initial), convert);
    }

    private TemporalField(TextField textField, ObjectProperty<TimePattern> pattern, Function<Duration, T> convert) {
        super(textField, text -> {
            TimePattern.Result r = pattern.get().parse(text);
            return r.isValid() ? convert.apply(r.value()) : null;
        });
        this.pattern = pattern;
        this.timeOfDay = pattern.get().isTimeOfDay();
        updatePrompt();
        pattern.addListener((obs, o, n) -> {
            updatePrompt();
            revalidate();
        });
        revalidate();
    }

    // Use the pattern as the prompt unless the caller set their own
    private void updatePrompt() {
        TextField tf = getTextField();
        String current = tf.getPromptText();
        if (current == null || current.isEmpty() || current.equals(autoPrompt)) {
            autoPrompt = pattern.get().promptText();
            tf.setPromptText(autoPrompt);
        }
    }

    public ObjectProperty<TimePattern> timePatternProperty() { return pattern; }
    public TimePattern getTimePattern() { return pattern.get(); }

    /** Switches to a new pattern, e.g. "[H:]MM:ss[.SSS]". */
    public void setPattern(String p) {
        pattern.set(new TimePattern(p, timeOfDay));
    }
}
