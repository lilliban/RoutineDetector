package it.unicam.routinedetector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class CommunitiesParser {

    public static Map<String, String> loadActivityToCommunity(String path) throws IOException {
        Map<String, String> map = new HashMap<>();

        for (String line : Files.readAllLines(Paths.get(path))) {

            line = line.trim();
            if (line.isEmpty()) continue;

            // ogni riga è fromattata con:
            // community_name:id activity1,activity2,...
            // a sx community_name:id poi uno spazio e poi l'elenco delle attività separate da virgola
            String[] parts = line.split("\t", 2);
            if (parts.length < 2) continue;

            String community = parts[0].split(":")[0];
            String[] activities = parts[1].split(",");

            for (String activity : activities) {
                map.put(activity.trim(), community);
            }
        }

        return map;
    }
}