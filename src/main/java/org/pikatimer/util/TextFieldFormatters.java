/*
 * Copyright (C) 2024 John Garner <segfaultcoredump@gmail.com>
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

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalTime;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import javafx.scene.control.TextField;
import javafx.scene.control.TextFormatter;
import javafx.scene.control.TextFormatter.Change;

/**
 * One-call setup for TextFields that hold times, durations, integers or
 * decimals. Each method installs a keystroke filter and a red highlight that
 * shows while the text is partial or invalid, and returns a
 * {@link ValidatedField} with the live parsed value and validity:
 * <pre>
 *   TemporalField&lt;LocalTime&gt; start = TextFieldFormatters.localTimeField(startTextField, "HH:MM[:ss[.SSS]]");
 *   okButton.disableProperty().bind(start.validProperty().not());
 *   ... start.getValue() ...
 * </pre>
 * See {@link TimePattern} for the pattern syntax.
 * <p>
 * Time fields make the delimiters easy to type. A ':' (or '.') is added for you
 * when you keep typing digits past a full 2 digit unit, and space, ';', ',',
 * '/', '-' and ':' all become whichever delimiter fits where you are. '.'
 * starts the fraction when one fits there, and acts like a ':' otherwise.
 * Deleting is never blocked, and digits typed into the middle of the text go
 * in as-is, so inline edits work; the field just shows red until the time is
 * valid again.
 *
 * @author John Garner <segfaultcoredump@gmail.com>
 */
public final class TextFieldFormatters {

    private TextFieldFormatters() {
    }

    // ---- Times and durations ----

    /**
     * Sets up a time-of-day field, e.g. {@code localTimeField(tf, "HH:MM[:ss[.SSS]]")}.
     */
    public static TemporalField<LocalTime> localTimeField(TextField tf, String pattern) {
        TemporalField<LocalTime> f = new TemporalField<>(tf, new TimePattern(pattern, true),
                d -> LocalTime.ofNanoOfDay(d.toNanos()));
        tf.setTextFormatter(new TextFormatter<>(timeFilter(f::getTimePattern)));
        return f;
    }

    /**
     * Sets up a duration field, e.g. {@code durationField(tf, "[H:]MM:ss[.SSS]")}.
     * Start the pattern with "[-]" to allow negative durations.
     */
    public static TemporalField<Duration> durationField(TextField tf, String pattern) {
        TemporalField<Duration> f = new TemporalField<>(tf, new TimePattern(pattern, false), d -> d);
        tf.setTextFormatter(new TextFormatter<>(timeFilter(f::getTimePattern)));
        return f;
    }

    /**
     * The keystroke filter for time fields. The pattern is looked up on every
     * change so it can be swapped at runtime.
     */
    static UnaryOperator<Change> timeFilter(Supplier<TimePattern> patternSupplier) {
        return change -> {
            if (!change.isContentChange()) return change;
            String typed = change.getText();
            if (typed.isEmpty()) return change; // deletions are always allowed

            TimePattern tp = patternSupplier.get();
            String old = change.getControlText();
            int start = change.getRangeStart();
            int end = change.getRangeEnd();
            String after = old.substring(end);

            // Map each typed character onto what the pattern wants at that spot
            StringBuilder sb = new StringBuilder(old.substring(0, start));
            for (int i = 0; i < typed.length(); i++) {
                char c = typed.charAt(i);
                String prefix = sb.toString();
                if (Character.isDigit(c)) {
                    sb.append(c);
                } else if (c == '-' && prefix.isEmpty() && tp.negativeAllowed() && !after.startsWith("-")) {
                    sb.append(c);
                } else if (c == '.' || c == ',') {
                    if (tp.fractionLegalAfter(prefix)) sb.append('.');
                    else appendDelimiter(tp, sb);
                } else if (Character.isWhitespace(c) || ":;/-_".indexOf(c) >= 0) {
                    appendDelimiter(tp, sb);
                }
                // anything else (letters, etc) is dropped
            }
            String inserted = sb.substring(start);

            // Typing one digit at the end of a full unit adds the next delimiter for you
            if (typed.length() == 1 && Character.isDigit(typed.charAt(0))
                    && start == end && end == old.length()) {
                String prefix = old;
                if (tp.trailingSegmentFull(prefix)) {
                    if (tp.colonLegalAfter(prefix)) inserted = ":" + inserted;
                    else if (tp.fractionLegalAfter(prefix)) inserted = "." + inserted;
                    else return null; // nowhere for another digit to go
                }
            }

            if (inserted.isEmpty()) return null;
            if (!tp.structurallyOk(old.substring(0, start) + inserted + after)) return null;

            change.setText(inserted);
            int caret = start + inserted.length();
            change.selectRange(caret, caret);
            return change;
        };
    }

    // Space and friends become a ':' if one fits, otherwise the '.' for the fraction
    private static void appendDelimiter(TimePattern tp, StringBuilder sb) {
        String prefix = sb.toString();
        if (tp.colonLegalAfter(prefix)) sb.append(':');
        else if (tp.fractionLegalAfter(prefix)) sb.append('.');
    }

    // ---- Numbers ----

    /** Sets up a whole number field. */
    public static ValidatedField<Integer> integerField(TextField tf, boolean allowNegative) {
        ValidatedField<Integer> f = new ValidatedField<>(tf, TextFieldFormatters::parseInteger);
        tf.setTextFormatter(new TextFormatter<>(integerFilter(allowNegative)));
        f.revalidate();
        return f;
    }

    /** Sets up a decimal number field. */
    public static ValidatedField<BigDecimal> decimalField(TextField tf, boolean allowNegative) {
        ValidatedField<BigDecimal> f = new ValidatedField<>(tf, TextFieldFormatters::parseDecimal);
        tf.setTextFormatter(new TextFormatter<>(decimalFilter(allowNegative)));
        f.revalidate();
        return f;
    }

    /** Only lets digits (and a leading '-' if allowed) into a field. */
    public static UnaryOperator<Change> integerFilter(boolean allowNegative) {
        String regex = allowNegative ? "-?\\d*" : "\\d*";
        return change -> change.getControlNewText().matches(regex) ? change : null;
    }

    /** Only lets a decimal number (with a leading '-' if allowed) into a field. */
    public static UnaryOperator<Change> decimalFilter(boolean allowNegative) {
        String regex = allowNegative ? "-?\\d*(\\.\\d*)?" : "\\d*(\\.\\d*)?";
        return change -> change.getControlNewText().matches(regex) ? change : null;
    }

    /** The Integer in the text, or null if there isn't one. */
    public static Integer parseInteger(String s) {
        if (s == null) return null;
        try {
            return Integer.valueOf(s.strip());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** The BigDecimal in the text, or null if there isn't one. */
    public static BigDecimal parseDecimal(String s) {
        if (s == null) return null;
        try {
            return new BigDecimal(s.strip());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
