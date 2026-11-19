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
import java.util.ArrayList;
import java.util.List;

/**
 * A small time pattern language for time-of-day and duration text fields.
 * <p>
 * Tokens (case-insensitive):
 * <ul>
 * <li>{@code H} or {@code HH}: hours. 0-23 and at most 2 digits for a time of
 * day, unbounded for a duration (100+ hour cutoffs).</li>
 * <li>{@code MM}: minutes, 0-59</li>
 * <li>{@code ss}: seconds, 0-59</li>
 * <li>{@code .S...}: a fraction of a second, up to as many digits as there are S's</li>
 * <li>{@code [-]} at the very start: a leading minus sign is allowed</li>
 * <li>{@code [...]}: an optional section</li>
 * </ul>
 * Optional ":" segments must either all lead the pattern (right-aligned, e.g.
 * {@code [H:]MM:ss}, where "5:00" is 5 minutes) or all trail it (left-aligned,
 * e.g. {@code HH:MM[:ss[.SSS]]}, where "5:00" is 5 hours).
 * <p>
 * The first segment the user enters may have 1 or 2 digits (any number for a
 * duration, and with no upper limit on its value), every later segment needs
 * exactly 2 digits, and the fraction needs 1 to N digits.
 *
 * @author John Garner <segfaultcoredump@gmail.com>
 */
public final class TimePattern {

    public enum Status { EMPTY, VALID, INCOMPLETE, INVALID }

    /** The outcome of a parse. {@code value} is only set when the status is VALID. */
    public record Result(Status status, Duration value) {
        public boolean isValid() { return status == Status.VALID; }
    }

    private enum Unit { HOUR, MINUTE, SECOND }

    private record Segment(Unit unit, boolean optional) {}

    private final String pattern;
    private final boolean timeOfDay;
    private final boolean negativeAllowed;
    private final List<Segment> segments = new ArrayList<>();
    private final int fractionDigits;      // 0 = no fraction
    private final boolean fractionOptional;
    private final boolean rightAligned;    // optional segments lead the pattern
    private final int requiredSegments;

    /**
     * @param pattern the pattern, e.g. "HH:MM[:ss[.SSS]]"
     * @param timeOfDay true for a LocalTime (hours 0-23), false for a Duration
     * @throws IllegalArgumentException if the pattern can't be understood
     */
    public TimePattern(String pattern, boolean timeOfDay) {
        if (pattern == null || pattern.isBlank()) throw new IllegalArgumentException("Empty time pattern");
        this.pattern = pattern.strip();
        this.timeOfDay = timeOfDay;

        String p = this.pattern;
        boolean neg = false;
        if (p.startsWith("[-]")) {
            neg = true;
            p = p.substring(3);
        }
        if (neg && timeOfDay) throw new IllegalArgumentException("A time of day can't be negative: " + pattern);
        negativeAllowed = neg;

        int depth = 0;
        int fraction = 0;
        boolean fracOpt = false;
        boolean afterDot = false;
        int i = 0;
        while (i < p.length()) {
            char c = p.charAt(i);
            switch (c) {
                case '[' -> { depth++; i++; }
                case ']' -> {
                    if (--depth < 0) throw new IllegalArgumentException("Unbalanced ] in time pattern: " + pattern);
                    i++;
                }
                case ':' -> {
                    if (afterDot) throw new IllegalArgumentException("':' after the fraction in time pattern: " + pattern);
                    i++;
                }
                case '.' -> {
                    if (afterDot || segments.isEmpty()) throw new IllegalArgumentException("Misplaced '.' in time pattern: " + pattern);
                    afterDot = true;
                    fracOpt = depth > 0;
                    i++;
                }
                default -> {
                    char u = Character.toUpperCase(c);
                    if (u != 'H' && u != 'M' && u != 'S') throw new IllegalArgumentException("Unknown character '" + c + "' in time pattern: " + pattern);
                    int run = i;
                    while (run < p.length() && Character.toUpperCase(p.charAt(run)) == u) run++;
                    if (afterDot) {
                        if (u != 'S') throw new IllegalArgumentException("Only S may follow '.' in time pattern: " + pattern);
                        fraction = run - i;
                    } else {
                        Unit unit = switch (u) {
                            case 'H' -> Unit.HOUR;
                            case 'M' -> Unit.MINUTE;
                            default -> Unit.SECOND;
                        };
                        if (!segments.isEmpty() && segments.getLast().unit.ordinal() >= unit.ordinal())
                            throw new IllegalArgumentException("Time pattern units must go hours, minutes, seconds: " + pattern);
                        segments.add(new Segment(unit, depth > 0));
                    }
                    i = run;
                }
            }
        }
        if (depth != 0) throw new IllegalArgumentException("Unbalanced [ in time pattern: " + pattern);
        if (segments.isEmpty()) throw new IllegalArgumentException("No time units in pattern: " + pattern);
        if (afterDot && fraction == 0) throw new IllegalArgumentException("'.' with no S digits in time pattern: " + pattern);
        fractionDigits = fraction;
        fractionOptional = fracOpt;

        // Optional segments must be a contiguous prefix or a contiguous suffix
        int n = segments.size();
        int leading = 0;
        while (leading < n && segments.get(leading).optional) leading++;
        int trailing = 0;
        while (trailing < n - leading && segments.get(n - 1 - trailing).optional) trailing++;
        int required = n - leading - trailing;
        if (leading > 0 && trailing > 0)
            throw new IllegalArgumentException("Optional units must all lead or all trail the time pattern: " + pattern);
        for (int s = leading; s < n - trailing; s++) {
            if (segments.get(s).optional)
                throw new IllegalArgumentException("Optional units must all lead or all trail the time pattern: " + pattern);
        }
        if (required == 0) throw new IllegalArgumentException("A time pattern needs at least one required unit: " + pattern);
        rightAligned = leading > 0;
        requiredSegments = required;
    }

    public String pattern() { return pattern; }
    public boolean isTimeOfDay() { return timeOfDay; }
    public boolean negativeAllowed() { return negativeAllowed; }
    public int fractionDigits() { return fractionDigits; }

    /** The most ':' characters the text may hold. */
    public int maxColons() { return segments.size() - 1; }

    /** A prompt text built from the pattern, e.g. "HH:MM[:SS[.sss]]". */
    public String promptText() {
        StringBuilder sb = new StringBuilder();
        boolean afterDot = false;
        for (char c : pattern.toCharArray()) {
            if (c == '.') afterDot = true;
            if (Character.isLetter(c)) sb.append(afterDot ? Character.toLowerCase(c) : Character.toUpperCase(c));
            else sb.append(c);
        }
        return sb.toString();
    }

    /**
     * Parses the text. Blank text is EMPTY. Text that could still become valid
     * by typing more is INCOMPLETE. Anything else that does not parse is INVALID.
     */
    public Result parse(String text) {
        if (text == null || text.isBlank()) return new Result(Status.EMPTY, null);
        String t = text.strip();

        boolean negative = false;
        if (t.startsWith("-")) {
            if (!negativeAllowed) return invalid();
            negative = true;
            t = t.substring(1);
            if (t.isEmpty()) return incomplete();
        }

        String fraction = null;
        int dot = t.indexOf('.');
        if (dot >= 0) {
            fraction = t.substring(dot + 1);
            t = t.substring(0, dot);
            if (fractionDigits == 0 || fraction.indexOf('.') >= 0 || fraction.indexOf(':') >= 0) return invalid();
            if (!fraction.chars().allMatch(Character::isDigit)) return invalid();
            if (fraction.length() > fractionDigits) return invalid();
        }

        String[] parts = t.split(":", -1);
        int entered = parts.length;
        if (entered > segments.size()) return invalid();
        for (String part : parts) {
            if (!part.chars().allMatch(Character::isDigit)) return invalid();
        }

        // Map the entered parts onto the pattern's units
        int offset = rightAligned ? segments.size() - entered : 0;
        boolean incomplete = false;
        long nanos = 0;
        for (int i = 0; i < entered; i++) {
            String part = parts[i];
            Segment seg = segments.get(offset + i);
            boolean last = i == entered - 1;
            if (part.isEmpty()) {
                if (entered == 1 && fraction != null) continue; // ".5" is half a second
                if (last) { incomplete = true; continue; }
                return invalid();
            }
            long v;
            if (i == 0) {
                if (timeOfDay && part.length() > 2) return invalid();
                if (part.length() > 18) return invalid();
                v = Long.parseLong(part);
                if (timeOfDay && v > (seg.unit == Unit.HOUR ? 23 : 59)) return invalid();
            } else {
                if (part.length() > 2) return invalid();
                v = Long.parseLong(part);
                if (part.length() < 2) {
                    // "5:3" could still become "5:35"
                    if (last && v <= 5) { incomplete = true; continue; }
                    return invalid();
                }
                if (v > 59) return invalid();
            }
            nanos += v * switch (seg.unit) {
                case HOUR -> 3_600_000_000_000L;
                case MINUTE -> 60_000_000_000L;
                case SECOND -> 1_000_000_000L;
            };
        }

        if (fraction != null) {
            // A fraction only goes after the pattern's last unit
            if (offset + entered != segments.size()) return invalid();
            if (fraction.isEmpty()) incomplete = true;
            else nanos += Long.parseLong((fraction + "000000000").substring(0, 9));
        }

        if (entered < requiredSegments) incomplete = true;
        if (fraction == null && fractionDigits > 0 && !fractionOptional) incomplete = true;
        if (incomplete) return incomplete();

        Duration d = Duration.ofNanos(nanos);
        if (timeOfDay && d.compareTo(Duration.ofDays(1)) >= 0) return invalid();
        return new Result(Status.VALID, negative ? d.negated() : d);
    }

    // ---- Typing helpers used by the text field filter ----

    /** Text without the leading minus sign. */
    private static String unsigned(String text) {
        return text.startsWith("-") ? text.substring(1) : text;
    }

    private static int count(String s, char c) {
        int n = 0;
        for (int i = 0; i < s.length(); i++) if (s.charAt(i) == c) n++;
        return n;
    }

    /** The digits typed so far in the segment that ends the text. */
    static String trailingSegment(String text) {
        String u = unsigned(text);
        int cut = Math.max(u.lastIndexOf(':'), u.lastIndexOf('.'));
        return u.substring(cut + 1);
    }

    /** Can a ':' go right after this text? */
    boolean colonLegalAfter(String prefix) {
        String u = unsigned(prefix);
        if (u.isEmpty() || u.indexOf('.') >= 0 || u.endsWith(":")) return false;
        return count(u, ':') < maxColons();
    }

    /** Can a '.' (the start of the fraction) go right after this text? */
    boolean fractionLegalAfter(String prefix) {
        if (fractionDigits == 0) return false;
        String u = unsigned(prefix);
        if (u.isEmpty()) return rightAligned && requiredSegments == 1; // ".5" seconds
        if (u.indexOf('.') >= 0 || u.endsWith(":")) return false;
        // Left-aligned patterns only take a fraction after the last unit
        return rightAligned || count(u, ':') == maxColons();
    }

    /** Is the segment at the end of this text already as long as it can be? */
    boolean trailingSegmentFull(String prefix) {
        String u = unsigned(prefix);
        String seg = trailingSegment(prefix);
        if (u.indexOf('.') >= 0) return seg.length() >= fractionDigits;
        if (u.indexOf(':') < 0) return timeOfDay && seg.length() >= 2; // durations have no cap on the first unit
        return seg.length() >= 2;
    }

    /** The cheap shape checks a keystroke must pass. Range checks are left to {@link #parse}. */
    boolean structurallyOk(String text) {
        String u = text;
        if (text.startsWith("-")) {
            if (!negativeAllowed) return false;
            u = text.substring(1);
        }
        for (int i = 0; i < u.length(); i++) {
            char c = u.charAt(i);
            if (!(Character.isDigit(c) || c == ':' || c == '.')) return false;
        }
        if (count(u, ':') > maxColons()) return false;
        int dot = u.indexOf('.');
        if (dot >= 0) {
            if (fractionDigits == 0) return false;
            if (u.indexOf('.', dot + 1) >= 0 || u.indexOf(':', dot) >= 0) return false;
        }
        return true;
    }

    private static Result invalid() { return new Result(Status.INVALID, null); }
    private static Result incomplete() { return new Result(Status.INCOMPLETE, null); }

    @Override
    public String toString() { return pattern; }
}
