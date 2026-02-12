package it.unicam.routinedetector;

import java.time.Instant;

//evento arricchito
public class EnrichedEvent {
    public final Instant timestamp;
    public final String activity;
    public final String community;
    public final long eventId;

    public EnrichedEvent(Instant timestamp, String activity, String community, long eventId) {
        this.timestamp = timestamp;
        this.activity = activity;
        this.community = community;
        this.eventId = eventId;
    }

    public String toCsvLine() {
        return timestamp + "," + escape(activity) + "," + escape(community) + "," + eventId;
    }

    private static String escape(String s) {
        // nel tuo dataset non dovrebbero esserci virgole, ma teniamoci safe
        String t = s.replace("\"", "\"\"");
        if (t.contains(",") || t.contains("\"")) return "\"" + t + "\"";
        return t;
    }
}
