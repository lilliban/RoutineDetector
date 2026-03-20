package it.unicam.routinedetector;

import java.time.Instant;


public class Event {
    public Instant timestamp;
    public String activity;
    public long eventId;
    public String caseId;
    // per selezionare sono i complete
    public String lifecycle;

    public Event() {}

    public Event(Instant timestamp, String activity, long eventId, String caseId, String lifecycle) {
        this.timestamp = timestamp;
        this.activity = activity;
        this.eventId = eventId;
        this.caseId = caseId;
        this.lifecycle = lifecycle;
    }
    public String getActivity() { return activity; }
    public void setActivity(String activity) { this.activity = activity; }

    public String getCaseId() { return caseId; }
    public void setCaseId(String caseId) { this.caseId = caseId; }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }

    public long getEventId() { return eventId; }
    public void setEventId(long eventId) { this.eventId = eventId; }

    public String getLifecycle() { return lifecycle; }
    public void setLifecycle(String lifecycle) { this.lifecycle = lifecycle; }
}