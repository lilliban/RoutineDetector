package it.unicam.cs.taxiroutes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommunitiesParser {

    private static String clean(String s) {
        if (s == null) return "";
        return s.trim().replace("\"", "");
    }

    public static Map<String, String> buildLocationIdToZone(String path) throws IOException {
        Map<String, String> map = new HashMap<>();

        List<String> lines = Files.readAllLines(Paths.get(path));
        if (lines.isEmpty()) return map;


        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            if (line.isEmpty()) continue;

            String[] parts = line.split(",", 4);
            if (parts.length < 3) continue;

            String locationId = clean(parts[0]); // "161"
            String zone       = clean(parts[2]); // "Midtown Center"

            if (!locationId.isEmpty() && !zone.isEmpty()) {
                String borough = clean(parts[1]);
                if (!borough.equals("EWR") && !borough.equals("N/A") && !borough.equals("Unknown"))
                    map.put(locationId, zone);
            }
        }

        System.out.println("buildLocationIdToZone: " + map.size() + " entries");
        return map;
    }


    public static Map<String, String> loadActivityToCommunity(String path) throws IOException {
        Map<String, String> map = new HashMap<>();

        List<String> lines = Files.readAllLines(Paths.get(path));
        if (lines.isEmpty()) return map;

        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            if (line.isEmpty()) continue;

            String[] parts = line.split(",", 4);
            if (parts.length < 3) continue;

            String borough = clean(parts[1]);
            String zone    = clean(parts[2]);

            if (!borough.isEmpty() && !zone.isEmpty())
                if (!borough.equals("N/A") && !borough.equals("Unknown") && !borough.equals("EWR"))
                     map.put(zone, borough);
        }

        System.out.println("loadActivityToCommunity: " + map.size() + " entries");
        return map;
    }


    public static Map<String, List<String>> loadCommunities(String path) throws IOException {
        Map<String, List<String>> map = new HashMap<>();

        List<String> lines = Files.readAllLines(Paths.get(path));
        if (lines.isEmpty()) return map;

        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            if (line.isEmpty()) continue;

            String[] parts = line.split(",", 4);
            if (parts.length < 3) continue;

            String borough = clean(parts[1]);
            String zone    = clean(parts[2]);

            // salta community non significative
            if (borough.isEmpty() || zone.isEmpty()) continue;
            if (borough.equals("N/A") || borough.equals("Unknown") || borough.equals("EWR")) continue;

            map.computeIfAbsent(borough, k -> new ArrayList<>()).add(zone);
        }

        System.out.println("loadCommunities: " + map.size() + " communities");
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            System.out.println("  " + entry.getKey() + " → " + entry.getValue().size() + " zone");
        }

        return map;
    }
}