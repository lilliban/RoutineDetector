package it.unicam.routinedetector;

import java.time.Instant;

//evento arricchito
public class EnrichedEvent {
    public  Instant timestamp;
    public  String activity;
    public  String community;
    public  long eventId;

    public EnrichedEvent () {}

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
        // per sicurezza non si sa mai
        String t = s.replace("\"", "\"\"");
        if (t.contains(",") || t.contains("\"")) return "\"" + t + "\"";
        return t;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public String getActivity() {
        return activity;
    }

    public void setActivity(String activity) {
        this.activity = activity;
    }

    public String getCommunity() {
        return community;
    }

    public void setCommunity(String community) {
        this.community = community;
    }

    public long getEventId() {
        return eventId;
    }

    public void setEventId(long eventId) {
        this.eventId = eventId;
    }
}
