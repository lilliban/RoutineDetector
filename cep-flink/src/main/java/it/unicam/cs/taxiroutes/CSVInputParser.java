package it.unicam.cs.taxiroutes;

import org.apache.flink.api.common.functions.MapFunction;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class CSVInputParser implements MapFunction<String, Event> {

    private static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final int IDX_VENDOR   = 0;
    private static final int IDX_PICKUP   = 1;
    private static final int IDX_PU_LOC   = 7;

    private long eventCounter = 0;

    @Override
    public Event map(String line) throws Exception {
        if (line == null || line.isBlank()) return null;

        if (line.startsWith("VendorID")) return null;

        String[] f = line.split(",", -1);
        if (f.length <= IDX_PU_LOC) return null;

        String vendorId  = f[IDX_VENDOR].trim();
        String pickupRaw = f[IDX_PICKUP].trim();
        String puLocId   = f[IDX_PU_LOC].trim();

        if (vendorId.isEmpty() || pickupRaw.isEmpty() || puLocId.isEmpty()) return null;

        Instant ts = parseTimestamp(pickupRaw);
        if (ts == null) {
            System.out.println("DEBUG TIMESTAMP FAIL: '" + pickupRaw + "'");
            return null;
        }

        System.out.println("DEBUG PARSED OK: vendor=" + vendorId
                + " | ts=" + ts + " | loc=" + puLocId);

        return new Event(ts, "loc_" + puLocId, eventCounter++, vendorId, "complete");
    }

    private static Instant parseTimestamp(String s) {
        try {
            s = s.replace("\"", "").trim();
            return LocalDateTime.parse(s, FMT).toInstant(ZoneOffset.of("-05:00"));
        } catch (Exception e) {
            return null;
        }
    }
}