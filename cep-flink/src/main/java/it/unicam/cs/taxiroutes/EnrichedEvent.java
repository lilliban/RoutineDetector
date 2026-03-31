package it.unicam.cs.taxiroutes;

import java.time.Instant;

public class EnrichedEvent {

    public Instant timestamp;
    public String activity;
    public String community;
    public long eventId;
    public String caseId;

    public EnrichedEvent() {}

    public EnrichedEvent(Instant timestamp, String activity,
                         String community, long eventId, String caseId) {
        this.timestamp = timestamp;
        this.activity  = activity;
        this.community = community;
        this.eventId   = eventId;
        this.caseId    = caseId;
    }

    public String toCsvLine() {
        return timestamp + "," + escape(activity) + ","
                + escape(community) + "," + eventId + "," + escape(caseId);
    }

    private static String escape(String s) {
        if (s == null) return "";
        String t = s.replace("\"", "\"\"");
        if (t.contains(",") || t.contains("\"")) return "\"" + t + "\"";
        return t;
    }

    public Instant getTimestamp()  { return timestamp; }
    public String getActivity()    { return activity; }
    public String getCommunity()   { return community; }
    public long getEventId()       { return eventId; }
    public String getCaseId()      { return caseId; }

    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }
    public void setActivity(String activity)    { this.activity = activity; }
    public void setCommunity(String community)  { this.community = community; }
    public void setEventId(long eventId)        { this.eventId = eventId; }
    public void setCaseId(String caseId)        { this.caseId = caseId; }
}