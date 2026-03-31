package it.unicam.cs.taxiroutes;

import java.time.Instant;

public class Event {

    public Instant timestamp;
    public String activity;
    public long eventId;
    public String caseId;
    public String lifecycle;

    public Event() {}

public Event (Instant timestamp, String activity, long eventId, String caseId, String lifecycle) {
    this.timestamp = timestamp;
    this.activity = activity;
    this.eventId = eventId;
    this.caseId = caseId;
    this.lifecycle = lifecycle;
}

public Instant getTimestamp() {
    return timestamp;
}
public String getActivity() {
    return activity;
}
public long getEventId() {
    return eventId;
}
public String getCaseId() {
    return caseId;
}
public String getLifecycle() {
    return lifecycle;
}

public void setTimestamp(Instant timestamp) {
    this.timestamp = timestamp;
}
public void setActivity(String activity) {
    this.activity = activity;
}
public void setEventId(long eventId) {
    this.eventId = eventId;
}
public void setCaseId(String caseId) {
    this.caseId = caseId;
}
public void setLifecycle(String lifecycle) {
    this.lifecycle = lifecycle;
}
}
