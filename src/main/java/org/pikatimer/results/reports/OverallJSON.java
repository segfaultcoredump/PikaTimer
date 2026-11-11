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
package org.pikatimer.results.reports;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.SerializableString;
import com.fasterxml.jackson.core.io.CharacterEscapes;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.core.util.Separators;
import org.pikatimer.participant.CustomAttribute;
import org.pikatimer.participant.Participant;
import org.pikatimer.participant.ParticipantDAO;
import org.pikatimer.race.AwardCategory;
import org.pikatimer.race.AwardWinner;
import org.pikatimer.race.CourseRecord;
import org.pikatimer.race.Race;
import org.pikatimer.race.RaceDAO;
import org.pikatimer.results.ProcessedResult;
import org.pikatimer.results.RaceReport;
import org.pikatimer.results.RaceReportType;
import org.pikatimer.timing.CookedTimeData;
import org.pikatimer.timing.Segment;
import org.pikatimer.timing.Split;
import org.pikatimer.timing.TimingDAO;
import org.pikatimer.util.DurationFormatter;
import org.pikatimer.util.Pace;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Overall results as a JSON array, one object per participant.
 *
 * All values are written as JSON strings. By default values are written raw
 * (and JSON-escaped by Jackson). When embedded in the HTML5 report the values
 * are rendered as HTML, so {@link #setHtmlEscape(boolean)} switches to
 * HTML-entity-escaped values. In both modes the characters &lt; &gt; &amp; '
 * and U+2028/U+2029 are written as \\uXXXX escapes so the output can be
 * safely embedded in a &lt;script&gt; block.
 *
 * @author John Garner <segfaultcoredump@gmail.com>
 */
public class OverallJSON implements RaceReportType{
    private static final Logger logger = LoggerFactory.getLogger(OverallJSON.class);

    private static final JsonFactory JSON_FACTORY = new JsonFactory();
    private static final CharacterEscapes SCRIPT_SAFE_ESCAPES = new ScriptSafeEscapes();

    Race race;

        // Defaults
    Boolean showDQ = true;
    Boolean inProgress = false;
    Boolean showSplits = false;
    Boolean showSegments = false;
    Boolean showSegmentPace = false;
    Boolean showSegmentSplits = false;

    Boolean showDNF = true;
    Boolean showPace = true;
    Boolean showGun = true;
    Boolean showAwards = true;
    Boolean showCourseRecords = true;

    Boolean showCustomAttributes = false;
    List<CustomAttribute> customAttributesList = new ArrayList<>();

    Map<String,Boolean> supportedOptions = new HashMap<>();
    Map<String,CustomAttribute> flattenedCustomNames = new HashMap<>();

    private boolean htmlEscape = false;

    // Per-run state, set up at the start of process()
    private String dispFormat;
    private String dispTimestamp;
    private String roundMode;
    private Pace pace;
    private Duration cutoffTime;
    private String cutoffTimeString;
    private boolean segmentsToShow;
    private boolean penaltiesOrBonuses;
    private boolean onCourseOCO;
    private final Map<String,List<String>> awardWinnersByBibMap = new HashMap<>();
    private final Map<String,Map<AwardCategory,AwardWinner>> awardWinnersDetailByBibMap = new HashMap<>();

    public OverallJSON(){
        supportedOptions.put("showDQ", true);
        supportedOptions.put("inProgress", false);
        supportedOptions.put("showSplits", false);
        supportedOptions.put("showSegments", true);
        supportedOptions.put("showSegmentPace", false);
        supportedOptions.put("showSegmentSplits", false);
        supportedOptions.put("showCustomAttributes", false);
        supportedOptions.put("showDNF", false);
        supportedOptions.put("showPace", true);
        supportedOptions.put("showGun", true);
        supportedOptions.put("showAwards",true);
        supportedOptions.put("showCourseRecords", true);
    }

    /**
     * When true, string values are HTML-entity-escaped and tabs/line breaks
     * are flattened to spaces. Used by OverallHTML5, whose JavaScript inserts
     * the values directly into the page. Defaults to false (raw values).
     */
    public void setHtmlEscape(boolean htmlEscape){
        this.htmlEscape = htmlEscape;
    }

    /**
     * Prepare a user-supplied string for output. Jackson handles all JSON
     * escaping, so this only applies the optional HTML escaping.
     */
    private String text(String s){
        if (s == null) return "";
        if (!htmlEscape) return s;
        return StringEscapeUtils.escapeHtml4(s).replaceAll("\\t|\\R", " ");
    }

    /**
     * Build the custom attribute key. This must produce exactly the same keys
     * as OverallHTML5.escapeAndFlatten() since the HTML5 DataTables column
     * definitions reference "custom_" + key. So it keeps the original
     * HTML-escape-then-flatten logic regardless of the htmlEscape setting.
     */
    private String escapeAndFlatten(CustomAttribute a){
        String s = StringEscapeUtils.escapeHtml4(a.getName()).replace("'", "\\'").replace("\t", " ").replace("\\R", " ")
                .replace(" ", "_").replaceAll("[^A-Za-z0-9_]{1}", "_");
        if ((flattenedCustomNames.containsKey(s) && !flattenedCustomNames.get(s).equals(a)) || s.replace("_", "").isEmpty()) return a.getUUID().replaceAll("[^A-Za-z0-9_]", "_");
        flattenedCustomNames.put(s,a);
        return s;
    }

    @Override
    public void init(Race r) {
        race = r;
    }

    @Override
    public Boolean optionSupport(String feature) {
        return supportedOptions.containsKey(feature);
    }

    @Override
    public String process(List<ProcessedResult> prList, RaceReport rr) {
        logger.debug("OverallJSON.process() Called... ");

        race = rr.getRace();

        supportedOptions.keySet().forEach(k -> supportedOptions.put(k, rr.getBooleanAttribute(k)));

        showDQ = supportedOptions.get("showDQ");
        inProgress = supportedOptions.get("inProgress");
        showSplits = supportedOptions.get("showSplits");
        showSegments = supportedOptions.get("showSegments");
        showSegmentPace = supportedOptions.get("showSegmentPace");
        showSegmentSplits = supportedOptions.get("showSegmentSplits");
        showDNF = supportedOptions.get("showDNF");
        showPace = supportedOptions.get("showPace");
        showGun = supportedOptions.get("showGun");
        showAwards = supportedOptions.get("showAwards");
        showCourseRecords = supportedOptions.get("showCourseRecords");
        showCustomAttributes = supportedOptions.get("showCustomAttributes");

        dispFormat = race.getStringAttribute("TimeDisplayFormat");
        dispTimestamp = race.getStringAttribute("TimeDisplayFormat").replace("[","").replace("}","");
        roundMode = race.getStringAttribute("TimeRoundingMode");
        pace = Pace.valueOf(race.getStringAttribute("PaceDisplayFormat"));

        // we use the splits to display the segment splits
        if (showSegmentSplits) showSplits = true;

        if (showCustomAttributes) customAttributesList= ParticipantDAO.getInstance().getCustomAttributes().stream().filter(a -> {
            if (rr.getBooleanAttribute(a.getUUID()) != null )
                return rr.getBooleanAttribute(a.getUUID());
            return false;
        }).collect(Collectors.toList());
        if (showCustomAttributes && customAttributesList.isEmpty()) showCustomAttributes = false;

        cutoffTime = Duration.ofNanos(race.getRaceCutoff());
        cutoffTimeString = formatTime(cutoffTime);

        awardWinnersByBibMap.clear();
        awardWinnersDetailByBibMap.clear();
        if (showAwards){
            Map<AwardCategory,Map<String,List<AwardWinner>>>  awardWinnersMap = race.getAwards().getAwardWinners(prList);
            String dash = htmlEscape ? " &mdash; " : " \u2014 ";
            race.getAwards().awardCategoriesProperty().forEach(ac -> {
                if (!ac.getVisibleOverall()) return;
                Map<String,List<AwardWinner>> resultsMap = awardWinnersMap.get(ac);
                List<String> categories = resultsMap.keySet().stream().sorted((k1,k2) -> k1.compareTo(k2)).collect(Collectors.toList());
                categories.forEach(cat -> {
                    String description = (text(ac.getName()) + dash +  text(cat)).trim();
                    resultsMap.get(cat).forEach(w -> {
                        String bib = w.participant.getBib();
                        awardWinnersByBibMap.computeIfAbsent(bib, k -> new ArrayList<>())
                                .add(w.awardPlace + getOrdinal(w.awardPlace) + " " + description + " (Time: " + formatTime(w.awardTime) + ")");
                        awardWinnersDetailByBibMap.computeIfAbsent(bib, k -> new LinkedHashMap<>()).put(ac, w);
                    });
                });
            });
        }

        segmentsToShow = race.raceSegmentsProperty().stream().anyMatch(s -> !s.getHidden());
        penaltiesOrBonuses = prList.stream().anyMatch(s -> (s.getBonus() || s.getPenalty()));
        onCourseOCO = prList.stream().anyMatch(s -> s.getSplitOCO());

        StringWriter out = new StringWriter();
        try (JsonGenerator gen = JSON_FACTORY.createGenerator(out)) {
            gen.setCharacterEscapes(SCRIPT_SAFE_ESCAPES);
            gen.setPrettyPrinter(newPrettyPrinter());
            gen.writeStartArray();
            for (ProcessedResult pr : prList) writeResult(gen, pr);
            gen.writeEndArray();
        } catch (IOException ex) {
            // Can't really happen when writing to a StringWriter
            throw new UncheckedIOException(ex);
        }
        return out.toString() + "\n";
    }

    private void writeResult(JsonGenerator gen, ProcessedResult pr) throws IOException {
        // if they are a DNF or DQ swap out the placement stats
        boolean hideTime = false;
        boolean dnf = pr.getParticipant().getDNF();
        boolean dq = pr.getParticipant().getDQ();
        boolean oco = false;
        boolean splitOCO = pr.getSplitOCO();

        if (dq) hideTime = true;

        if (pr.getChipFinish() == null) dnf = true;

        if (!showDNF && !inProgress && dnf) return;
        if (!showDQ && dq) return;

        if (!cutoffTime.isZero() && ! dnf && !(cutoffTime.compareTo(pr.getChipFinish()) >= 0
                    || cutoffTimeString.equals(formatTime(pr.getChipFinish())))) {
            oco = true;
        }

        if (!showDNF && oco) return;

        gen.writeStartObject();

        writeParticipant(gen, pr);
        if (showCustomAttributes) writeCustomAttributes(gen, pr);
        if (penaltiesOrBonuses) writePenaltyBonus(gen, pr);
        if (onCourseOCO) gen.writeStringField("on_course_oco", pr.getSplitOCO().toString());
        if (showAwards) writeAwards(gen, pr);
        if (showCourseRecords) writeCourseRecords(gen, pr);
        writePlacement(gen, pr, splitOCO, dq, dnf, oco);
        if (showSplits) writeSplits(gen, pr, hideTime);
        if (showSegments && segmentsToShow) writeSegments(gen, pr, hideTime);
        if (inProgress) writeLastSeen(gen, pr);

        if (dnf || dq) hideTime = true;
        writeFinish(gen, pr, hideTime);

        gen.writeEndObject();
    }

    private void writeParticipant(JsonGenerator gen, ProcessedResult pr) throws IOException {
        Participant p = pr.getParticipant();
        String fullName = p.fullNameProperty().getValueSafe();
        String fullNameFilter = text(fullName);
        String strippedName = StringUtils.stripAccents(fullName);
        if (!fullName.equals(strippedName)) fullNameFilter += " " + text(strippedName);

        gen.writeStringField("bib", text(p.getBib()));
        gen.writeStringField("age", pr.getAge().toString());
        gen.writeStringField("sex", text(pr.getSex()));
        gen.writeStringField("ag", text(pr.getAGCode()));
        gen.writeStringField("full_name", text(fullName));
        gen.writeStringField("full_name_filter", fullNameFilter);
        gen.writeStringField("first_name", text(p.getFirstName()));
        gen.writeStringField("middle_name", text(p.getMiddleName()));
        gen.writeStringField("last_name", text(p.getLastName()));
        gen.writeStringField("city", text(p.getCity()));
        gen.writeStringField("state", text(p.getState()));
        gen.writeStringField("country", text(p.getCountry()));
        gen.writeStringField("note", text(p.getNote()));
    }

    private void writeCustomAttributes(JsonGenerator gen, ProcessedResult pr) throws IOException {
        for (CustomAttribute a : customAttributesList) {
            gen.writeStringField("custom_" + escapeAndFlatten(a), text(pr.getParticipant().getCustomAttribute(a.getID()).getValueSafe()));
        }
    }

    private void writePenaltyBonus(JsonGenerator gen, ProcessedResult pr) throws IOException {
        gen.writeStringField("penalty", pr.getPenalty().toString());
        gen.writeStringField("bonus", pr.getBonus().toString());
        gen.writeStringField("penalty_bonus_note", text(pr.getBonusPenaltyNote()));
        gen.writeStringField("raw_chip_time", formatTime(pr.getRawChipFinishTime()));
        gen.writeStringField("raw_gun_time", formatTime(pr.getRawGunFinishTime()));
        gen.writeStringField("penalty_time", formatTime(pr.getPenaltyTime()));
        gen.writeStringField("bonus_time", formatTime(pr.getBonusTime()));
    }

    private void writeAwards(JsonGenerator gen, ProcessedResult pr) throws IOException {
        String bib = pr.getParticipant().getBib();
        if (!awardWinnersByBibMap.containsKey(bib)) {
            gen.writeStringField("award_winner", "no");
            return;
        }
        gen.writeStringField("award_winner", "yes");

        // Simple list of awards (descriptions are already escaped as needed)
        gen.writeObjectFieldStart("awards");
        List<String> awards = awardWinnersByBibMap.get(bib);
        for (int i = 0; i < awards.size(); i++) {
            gen.writeStringField("award_" + i, awards.get(i));
        }
        gen.writeEndObject();

        // Award Detail
        gen.writeObjectFieldStart("award_Detail");
        for (Map.Entry<AwardCategory,AwardWinner> e : awardWinnersDetailByBibMap.get(bib).entrySet()) {
            AwardCategory ac = e.getKey();
            AwardWinner a = e.getValue();
            gen.writeObjectFieldStart("award_" + text(ac.getName()));
            gen.writeStringField("title", text(ac.getName()));
            gen.writeStringField("category", text(a.awardTitle));
            gen.writeStringField("place", a.awardPlace.toString());
            gen.writeStringField("time", formatTime(a.awardTime));
            gen.writeEndObject();
        }
        gen.writeEndObject();
    }

    private void writeCourseRecords(JsonGenerator gen, ProcessedResult pr) throws IOException {
        List<CourseRecord> records = pr.getCourseRecords();
        if (records.isEmpty()) {
            gen.writeStringField("course_records", "no");
            return;
        }
        gen.writeStringField("course_records", "yes");
        gen.writeObjectFieldStart("course_record_detail");
        for (CourseRecord cr : records) {
            String segment = (cr.getSegmentID() == null ? "Overall" : cr.segmentProperty().get().getSegmentName());
            gen.writeObjectFieldStart(text(segment + "_" + cr.getSex() + "_" + cr.getCategory()));
            gen.writeStringField("segment", text(segment));
            gen.writeStringField("category", text(cr.getCategory()));
            gen.writeStringField("old_time", formatTime(cr.getRecordDuration()));
            gen.writeStringField("old_name", text(cr.getName()));
            gen.writeStringField("old_year", text(cr.getYear()));
            gen.writeStringField("old_city", text(cr.getCity()));
            gen.writeStringField("old_state", text(cr.getState()));
            gen.writeStringField("old_country", text(cr.getCountry()));
            gen.writeStringField("new_time", formatTime(cr.currentRecordTime()));
            gen.writeEndObject();
        }
        gen.writeEndObject();
    }

    private void writePlacement(JsonGenerator gen, ProcessedResult pr, boolean splitOCO, boolean dq, boolean dnf, boolean oco) throws IOException {
        if (splitOCO) {
            writePlaces(gen, "OCO", "~~", "~~");
            gen.writeStringField("oco_split", text(RaceDAO.getInstance().getSplitByID(pr.getOCOSplit()).getSplitName()));
            gen.writeStringField("oco_time", formatTime(pr.getOCOTime()));
            gen.writeStringField("oco_cutoff_time", formatTime(pr.getOCOCutoffTime()));
        } else if (dq) {
            writePlaces(gen, "DQ", "~~", "~~");
        } else if (inProgress && pr.getChipFinish() == null) {
            writePlaces(gen, "Started", "~~", "~~");
        } else if (!dnf) {
            if (!oco) writePlaces(gen, pr.getOverall().toString(), pr.getSexPlace().toString(), pr.getAGPlace().toString());
            else writePlaces(gen, "OCO", "~~", "~~");
        } else {
            writePlaces(gen, "DNF", "~~", "~~");
        }
    }

    private void writePlaces(JsonGenerator gen, String overall, String sex, String ag) throws IOException {
        gen.writeStringField("oa_place", overall);
        gen.writeStringField("sex_place", sex);
        gen.writeStringField("ag_place", ag);
    }

    private void writeSplits(JsonGenerator gen, ProcessedResult pr, boolean hideTime) throws IOException {
        boolean showSegmentSplitPace = showSegments && segmentsToShow && showSegmentPace && showSegmentSplits;
        List<Split> splits = race.splitsProperty();

        gen.writeObjectFieldStart("splits");
        for (int i = 2; i <= splits.size(); i++) {
            gen.writeObjectFieldStart("split_" + (i-1));
            Duration splitTime = pr.getSplit(i);
            if (hideTime || splitTime == null) {
                gen.writeStringField("display", "");
                gen.writeStringField("delta_time", "");
                if (showPace) gen.writeStringField("pace", "");
                if (showSegmentSplitPace) {
                    for (Segment seg : race.getSegments()) {
                        if (!segmentCoversSplit(seg, i)) continue;
                        gen.writeStringField(segmentPaceKey(seg), "~~");
                    }
                }
                gen.writeStringField("sort", "~~");
            } else {
                gen.writeStringField("display", formatTime(splitTime));

                Split thisSplit = splits.get(i-1);
                boolean zeroDistance = thisSplit.getSplitDistance().compareTo(BigDecimal.ZERO) == 0;
                if (i == 2) { // 1st split after start
                    float distance = thisSplit.getSplitDistance().floatValue();
                    gen.writeStringField("delta_time", formatTime(splitTime));
                    if (showPace) {
                        if (zeroDistance) gen.writeStringField("pace", "~~");
                        else gen.writeStringField("pace", pace.getPace(distance, race.getRaceDistanceUnits(), splitTime));
                    }
                    if (showSegmentSplitPace) {
                        for (Segment seg : race.getSegments()) {
                            if (!segmentCoversSplit(seg, i)) continue;
                            if (zeroDistance) gen.writeStringField(segmentPaceKey(seg), "~~");
                            else gen.writeStringField(segmentPaceKey(seg), segmentPace(seg).getPace(distance, race.getRaceDistanceUnits(), splitTime));
                        }
                    }
                } else {
                    Split previousSplit = splits.get(i-2);
                    Duration previousTime = pr.getSplit(i-1);
                    Duration delta = previousTime == null ? null : splitTime.minus(previousTime);
                    float distance = thisSplit.getSplitDistance().subtract(previousSplit.getSplitDistance()).floatValue();

                    gen.writeStringField("delta_time", delta == null ? "" : formatTime(delta));

                    if (showPace) {
                        if (thisSplit.getSplitDistance().compareTo(previousSplit.getSplitDistance()) == 0 || delta == null)
                            gen.writeStringField("pace", "");
                        else gen.writeStringField("pace", pace.getPace(distance, race.getRaceDistanceUnits(), delta));
                    }
                    if (showSegmentSplitPace) {
                        for (Segment seg : race.getSegments()) {
                            if (!segmentCoversSplit(seg, i)) continue;
                            if (zeroDistance || delta == null) gen.writeStringField(segmentPaceKey(seg), "~~");
                            else gen.writeStringField(segmentPaceKey(seg), segmentPace(seg).getPace(distance, race.getRaceDistanceUnits(), delta));
                        }
                    }
                }
                gen.writeStringField("sort", formatTimestamp(splitTime));
            }
            gen.writeEndObject();
        }
        gen.writeEndObject();
    }

    private boolean segmentCoversSplit(Segment seg, int splitPosition) {
        return !seg.getHidden() && seg.getStartSplitPosition() <= splitPosition && seg.getEndSplitPosition() >= splitPosition;
    }

    private String segmentPaceKey(Segment seg) {
        return "segment_" + seg.getID() + "_pace";
    }

    private Pace segmentPace(Segment seg) {
        return seg.getUseCustomPace() ? seg.getCustomPace() : pace;
    }

    private void writeSegments(JsonGenerator gen, ProcessedResult pr, boolean hideTime) throws IOException {
        gen.writeObjectFieldStart("segments");
        for (Segment seg : race.getSegments()) {
            if (seg.getHidden()) continue;
            gen.writeObjectFieldStart("segment_" + seg.getID());
            Duration segmentTime = pr.getSegmentTime(seg.getID());
            boolean noTime = hideTime || segmentTime == null;
            if (noTime) {
                gen.writeStringField("display", "");
                gen.writeStringField("sort", "~~");
                writePlaces(gen, "~~", "~~", "~~");
            } else {
                gen.writeStringField("display", formatTime(segmentTime));
                gen.writeStringField("sort", formatTimestamp(segmentTime));
                writePlaces(gen,
                        String.valueOf(pr.getSegmentOverallPlace(seg.getID())),
                        String.valueOf(pr.getSegmentSexPlace(seg.getID())),
                        String.valueOf(pr.getSegmentAGPlace(seg.getID())));
            }
            if (showSegmentPace) {
                if (noTime || (seg.getUseCustomPace() && Pace.NONE.equals(seg.getCustomPace())))
                    gen.writeStringField("pace", "~~");
                else gen.writeStringField("pace", segmentPace(seg).getPace(seg.getSegmentDistance(), race.getRaceDistanceUnits(), segmentTime));
            }
            gen.writeEndObject();
        }
        gen.writeEndObject();
    }

    private void writeLastSeen(JsonGenerator gen, ProcessedResult pr) throws IOException {
        TimingDAO td = TimingDAO.getInstance();
        List<CookedTimeData> times = td.getCookedTimesByBib(pr.getParticipant().getBib());
        if (times == null || times.isEmpty()) {
            gen.writeStringField("last_seen", "Never");
            return;
        }
        // Sort a copy so we don't reorder the DAO's list
        List<CookedTimeData> sorted = new ArrayList<>(times);
        sorted.sort((p1, p2) -> p1.getTimestamp().compareTo(p2.getTimestamp()));
        CookedTimeData lastChip = sorted.get(sorted.size()-1);
        gen.writeStringField("last_seen", text(td.getTimingLocationByID(lastChip.getTimingLocationId()).getLocationName() + " at " + formatTimestamp(lastChip.getTimestamp())));
    }

    private void writeFinish(JsonGenerator gen, ProcessedResult pr, boolean hideTime) throws IOException {
        gen.writeStringField("start_display", formatTime(pr.getChipStartTime()));

        // chip time
        if (hideTime) {
            gen.writeStringField("finish_split_delta", "");
            if (showPace) gen.writeStringField("finish_split_pace", "");
            gen.writeStringField("finish_display", "");
            gen.writeStringField("finish_sort", "~~");
        } else {
            if (showSplits) {
                List<Split> splits = race.splitsProperty();
                Duration lastSplitTime = pr.getSplit(splits.size()-1);
                if (lastSplitTime != null) {
                    // Calc distance and time since last split
                    Duration last = pr.getChipFinish().minus(lastSplitTime);
                    BigDecimal lastSplitDistance = splits.get(splits.size()-2).getSplitDistance();

                    gen.writeStringField("finish_split_delta", formatTime(last));
                    if (showPace) {
                        if (race.getRaceDistance().compareTo(lastSplitDistance) != 0)
                            gen.writeStringField("finish_split_pace", pace.getPace(race.getRaceDistance().floatValue() - lastSplitDistance.floatValue(), race.getRaceDistanceUnits(), last));
                        else gen.writeStringField("finish_split_pace", "");
                    }
                } else {
                    gen.writeStringField("finish_split_delta", "");
                    if (showPace) gen.writeStringField("finish_split_pace", "");
                }
            }
            gen.writeStringField("finish_display", formatTime(pr.getChipFinish()));
            gen.writeStringField("finish_sort", formatTimestamp(pr.getChipFinish()));
        }

        if (showGun) {
            if (hideTime) {
                gen.writeStringField("gun_display", "");
                gen.writeStringField("gun_sort", "~~");
            } else {
                gen.writeStringField("gun_display", formatTime(pr.getGunFinish()));
                gen.writeStringField("gun_sort", formatTimestamp(pr.getGunFinish()));
            }
        }

        if (showPace) {
            if (hideTime) gen.writeStringField("finish_pace", "~~");
            else gen.writeStringField("finish_pace", pace.getPace(race.getRaceDistance().floatValue(), race.getRaceDistanceUnits(), pr.getChipFinish()));
        }
    }

    private String formatTime(Duration d) {
        return DurationFormatter.durationToString(d, dispFormat, roundMode);
    }

    private String formatTimestamp(Duration d) {
        return DurationFormatter.durationToString(d, dispTimestamp, roundMode);
    }

    private static DefaultPrettyPrinter newPrettyPrinter() {
        DefaultIndenter indenter = new DefaultIndenter("  ", "\n");
        return new DefaultPrettyPrinter()
                .withSeparators(Separators.createDefaultInstance()
                        .withObjectFieldValueSpacing(Separators.Spacing.AFTER))
                .withObjectIndenter(indenter)
                .withArrayIndenter(indenter);
    }

    /**
     * Standard JSON escaping, plus \\uXXXX escapes for &lt; &gt; &amp; ' and
     * the U+2028/U+2029 line terminators so the JSON can be dropped into an
     * HTML &lt;script&gt; block without ending it early or breaking older
     * JavaScript parsers.
     */
    private static final class ScriptSafeEscapes extends CharacterEscapes {
        private static final SerializedString LINE_SEPARATOR = new SerializedString("\\u2028");
        private static final SerializedString PARAGRAPH_SEPARATOR = new SerializedString("\\u2029");
        private final int[] asciiEscapes;

        ScriptSafeEscapes() {
            asciiEscapes = CharacterEscapes.standardAsciiEscapesForJSON();
            asciiEscapes['<'] = CharacterEscapes.ESCAPE_STANDARD;
            asciiEscapes['>'] = CharacterEscapes.ESCAPE_STANDARD;
            asciiEscapes['&'] = CharacterEscapes.ESCAPE_STANDARD;
            asciiEscapes['\''] = CharacterEscapes.ESCAPE_STANDARD;
        }

        @Override
        public int[] getEscapeCodesForAscii() {
            return asciiEscapes;
        }

        @Override
        public SerializableString getEscapeSequence(int ch) {
            if (ch == 0x2028) return LINE_SEPARATOR;
            if (ch == 0x2029) return PARAGRAPH_SEPARATOR;
            return null;
        }
    }

    // I wonder how many times this has been "reinvented"....
    public static String getOrdinal(int value) {
        // if it ends in the 'teens
        if((value % 100 - value % 10) == 10) {
         return "th";
        }

        switch (value % 10) {
            case 1:
             return "st";
            case 2:
             return "nd";
            case 3:
             return "rd";
            default:
             return "th";
        }
    }

}
