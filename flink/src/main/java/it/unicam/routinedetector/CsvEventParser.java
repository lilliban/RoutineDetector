package it.unicam.routinedetector;

import org.apache.flink.api.common.functions.MapFunction;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

// trasforma da CSV a oggetto Event
// MapFunction<IN, OUT> è un’interfaccia di Flink e poi devi avere public Event map(String value)
public class CsvEventParser implements MapFunction<String, Event> {

    private boolean headerParsed = false;
    private final Map<String, Integer> idx = new HashMap<>();
    private char delimiter = '\t';


    private Integer tsI;
    private Integer actI;
    private Integer idI;


    private Integer lifecycleI;

    @Override
    public Event map(String line) {
        if (line == null) return null;
        line = line.strip();
        if (line.isEmpty()) return null;


        if (!headerParsed) {

            delimiter = (count(line, '\t') > count(line, ',')) ? '\t' : ',';

            String[] cols = split(line, delimiter);
            for (int i = 0; i < cols.length; i++) {
                idx.put(cols[i].trim(), i);
            }

            headerParsed = true;


            tsI = idx.get("time:timestamp");
            actI = idx.get("Activity");
            idI = idx.get("eventId");


            lifecycleI = idx.get("lifecycle:transition");

            return null;
        }

        String[] fields = split(line, delimiter);

        if (tsI == null || actI == null) return null;
        if (tsI >= fields.length || actI >= fields.length) return null;

        String tsRaw = fields[tsI].trim();
        String activity = fields[actI].trim();

        long eventId = -1;
        if (idI != null && idI < fields.length) {
            try {
                eventId = Long.parseLong(fields[idI].trim());
            }  catch (Exception ignored) {}
        }


        String lifecycle = null;
        if (lifecycleI != null && lifecycleI < fields.length) {
            lifecycle = fields[lifecycleI].trim();
        }


        Instant ts = parseTimestamp(tsRaw);
        if (ts == null) return null;

        return new Event(ts, activity, eventId, "house_1", lifecycle);
    }

    private static Instant parseTimestamp(String s) {
        try {
            String normalized = s.replace(" ", "T");
            return OffsetDateTime.parse(normalized, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant();
        } catch (Exception e) {
            return null;
        }
    }

    private static int count(String s, char c) {
        int n = 0;
        for (int i = 0; i < s.length(); i++) if (s.charAt(i) == c) n++;
        return n;
    }

    private static String[] split(String line, char delim) {
        return line.split(java.util.regex.Pattern.quote(String.valueOf(delim)), -1);
    }
}
