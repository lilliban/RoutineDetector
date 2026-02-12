package it.unicam.routinedetector;

import org.apache.flink.api.common.functions.MapFunction;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

//trasforma da CSV a oggetto Event
//MapFunction<IN, OUT> è un’interfaccia di Flink e poi devi avere public Event map(String value)
public class CsvEventParser implements MapFunction<String, Event> {

    private boolean headerParsed = false;
    private Map<String, Integer> idx = new HashMap<>();
    private char delimiter = '\t';

    @Override
    public Event map(String line) {
        if (line == null) return null;
        line = line.strip();
        if (line.isEmpty()) return null;

        // detect delimiter once using header
        if (!headerParsed) {
            delimiter = (count(line, '\t') > count(line, ',')) ? '\t' : ',';
            String[] cols = split(line, delimiter);
            for (int i = 0; i < cols.length; i++) {
                idx.put(cols[i].trim(), i);
            }
            headerParsed = true;
            return null; // header row
        }

        String[] fields = split(line, delimiter);

        // We need: time:timestamp, Activity, eventId (optional)
        Integer tsI = idx.get("time:timestamp");
        Integer actI = idx.get("Activity");
        Integer idI = idx.get("eventId");

        if (tsI == null || actI == null) return null;
        if (tsI >= fields.length || actI >= fields.length) return null;

        String tsRaw = fields[tsI].trim();
        String activity = fields[actI].trim();
        long eventId = -1;

        if (idI != null && idI < fields.length) {
            try { eventId = Long.parseLong(fields[idI].trim()); } catch (Exception ignored) {}
        }

        Instant ts = parseTimestamp(tsRaw);
        if (ts == null) return null;

        return new Event(ts, activity, eventId);
    }

    private static Instant parseTimestamp(String s) {
        try {
            // examples:
            // 2020-03-16 00:00:00+00:00
            String normalized = s.replace(" ", "T"); // 2020-03-16T00:00:00+00:00
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
        // Split semplice (qui non abbiamo virgolette complesse nel dataset)
        return line.split(java.util.regex.Pattern.quote(String.valueOf(delim)), -1);
    }
}
