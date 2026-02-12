package it.unicam.routinedetector;

import java.time.Instant;

// evento base, è immutabile
public class Event {
    public final Instant timestamp;
    public final String activity;
    public final long eventId;

    public Event(Instant timestamp, String activity, long eventId) {
        this.timestamp = timestamp;
        this.activity = activity;
        this.eventId = eventId;
    }
}
