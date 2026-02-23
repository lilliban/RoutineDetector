package it.unicam.routinedetector;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DetectedRoutine implements Serializable {

    private String routineName;
    private String strategy;
    private String caseId;
    private long startTimestampMillis;
    private long endTimestampMillis;


    private ArrayList<String> matchedActivities;

    public DetectedRoutine() {}

    public DetectedRoutine(String routineName,
                           String strategy,
                           String caseId,
                           long startTimestampMillis,
                           long endTimestampMillis,
                           List<String> matchedActivities) {
        this.routineName = routineName;
        this.strategy = strategy;
        this.caseId = caseId;
        this.startTimestampMillis = startTimestampMillis;
        this.endTimestampMillis = endTimestampMillis;
        this.matchedActivities = new ArrayList<>(matchedActivities); // copia “safe”
    }

    public String getRoutineName() { return routineName; }
    public String getStrategy() { return strategy; }
    public String getCaseId() { return caseId; }
    public long getStartTimestampMillis() { return startTimestampMillis; }
    public long getEndTimestampMillis() { return endTimestampMillis; }
    public ArrayList<String> getMatchedActivities() { return matchedActivities; }

    public void setRoutineName(String routineName) { this.routineName = routineName; }
    public void setStrategy(String strategy) { this.strategy = strategy; }
    public void setCaseId(String caseId) { this.caseId = caseId; }
    public void setStartTimestampMillis(long v) { this.startTimestampMillis = v; }
    public void setEndTimestampMillis(long v) { this.endTimestampMillis = v; }
    public void setMatchedActivities(ArrayList<String> matchedActivities) {
        this.matchedActivities = matchedActivities;
    }

    public String toCsvLine() {
        return routineName + "," + strategy + "," + caseId + "," +
                Instant.ofEpochMilli(startTimestampMillis) + "," +
                Instant.ofEpochMilli(endTimestampMillis) + "," +
                String.join(";", matchedActivities);
    }

    @Override
    public String toString() {
        return "DetectedRoutine{" +
                "routineName='" + routineName + '\'' +
                ", strategy='" + strategy + '\'' +
                ", caseId='" + caseId + '\'' +
                ", start=" + Instant.ofEpochMilli(startTimestampMillis) +
                ", end=" + Instant.ofEpochMilli(endTimestampMillis) +
                ", activities=" + matchedActivities +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DetectedRoutine)) return false;
        DetectedRoutine that = (DetectedRoutine) o;
        return startTimestampMillis == that.startTimestampMillis &&
                endTimestampMillis == that.endTimestampMillis &&
                Objects.equals(routineName, that.routineName) &&
                Objects.equals(strategy, that.strategy) &&
                Objects.equals(caseId, that.caseId) &&
                Objects.equals(matchedActivities, that.matchedActivities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(routineName, strategy, caseId,
                startTimestampMillis, endTimestampMillis, matchedActivities);
    }
}
