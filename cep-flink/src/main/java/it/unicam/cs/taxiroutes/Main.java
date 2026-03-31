package it.unicam.cs.taxiroutes;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws Exception {

        Args cfg = Args.parse(args);

        System.out.println("Events:      " + cfg.eventsPath);
        System.out.println("Communities: " + cfg.communitiesPath);
        System.out.println("Output dir:  " + cfg.outputDir);
        System.out.println("Strategy:    " + cfg.strategy);
        System.out.println("Mode:        " + cfg.mode);


        Map<String, String> locationIdToZone =
                CommunitiesParser.buildLocationIdToZone(cfg.communitiesPath);
        System.out.println("Loaded locationId->zone: " + locationIdToZone.size());

        Map<String, String> activityToCommunity =
                CommunitiesParser.loadActivityToCommunity(cfg.communitiesPath);
        System.out.println("Loaded activity->community: " + activityToCommunity.size());

        Map<String, List<String>> communities =
                CommunitiesParser.loadCommunities(cfg.communitiesPath);
        System.out.println("Loaded communities: " + communities.size());
        for (Map.Entry<String, List<String>> entry : communities.entrySet()) {
            System.out.println("  " + entry.getKey() + " → " + entry.getValue().size() + " zone");
        }


        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(cfg.eventsPath))
                .build();

        DataStream<String> lines = env.fromSource(
                        source, WatermarkStrategy.noWatermarks(), "csv-source")
                .filter(line -> line != null
                        && !line.isBlank()
                        && !line.startsWith("VendorID"))
                .name("skip-header");


        DataStream<Event> events = lines
                .map(new CSVInputParser())
                .name("parse-csv")
                .filter(e -> e != null)
                .name("drop-null")
                .filter(e -> "complete".equals(e.lifecycle))
                .name("keep-only-complete");


        DataStream<EnrichedEvent> enriched = events
                .map(e -> {
                    String locationId = e.activity.replace("loc_", "").trim();
                    String zoneName   = locationIdToZone.get(locationId);
                    if (zoneName == null) return null;
                    String community  = activityToCommunity.get(zoneName);
                    if (community == null) return null;
                    return new EnrichedEvent(e.timestamp, zoneName, community,
                            e.eventId, e.caseId);
                })
                .name("enrich-community")
                .filter(x -> x != null)
                .name("drop-unknown");


        if ("enrichment".equalsIgnoreCase(cfg.mode)) {
            System.out.println("Running in ENRICHMENT mode");

            enriched
                    .map(EnrichedEvent::toCsvLine)
                    .name("to-csv-line")
                    .print()
                    .name("print-enriched");

        } else {
            System.out.println("Running in DETECTION mode");

            DataStream<EnrichedEvent> withWatermarks = enriched
                    .assignTimestampsAndWatermarks(
                            WatermarkStrategy
                                    .<EnrichedEvent>forMonotonousTimestamps()
                                    .withTimestampAssigner((event, ts) ->
                                            event.timestamp.toEpochMilli())
                    );

            for (Map.Entry<String, List<String>> entry : communities.entrySet()) {
                String community        = entry.getKey();
                List<String> activities = entry.getValue();

                System.out.println("  - Detecting: " + community
                        + " (" + activities.size() + " zone, strategy " + cfg.strategy + ")");

                DataStream<DetectedRoutine> detected = CEPDetector.detectSimple(
                        withWatermarks, community,
                        Strategy.valueOf(cfg.strategy),
                        activities, 0.8f,
                        Duration.ofHours(24)
                );

                detected
                        .map(DetectedRoutine::toCsvLine)
                        .name("to-csv-" + community)
                        .print()
                        .name("print-" + community);
            }
        }

        env.execute("TaxiRoutes - " + cfg.mode);
        System.out.println("Esecuzione completata!");
        System.exit(0);
    }


    static class Args {
        final String eventsPath;
        final String communitiesPath;
        final String outputDir;
        final String mode;
        final String strategy;

        Args(String eventsPath, String communitiesPath,
             String outputDir, String mode, String strategy) {
            this.eventsPath      = eventsPath;
            this.communitiesPath = communitiesPath;
            this.outputDir       = outputDir;
            this.mode            = mode;
            this.strategy        = strategy;
        }

        static Args parse(String[] args) {
            String events = null, comm = null, out = null,
                    mode = "detection", strategy = null;
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--events"      -> events   = args[++i];
                    case "--communities" -> comm     = args[++i];
                    case "--out"         -> out      = args[++i];
                    case "--strategy"    -> strategy = args[++i];
                    case "--mode"        -> mode     = args[++i];
                }
            }
            if (events == null || comm == null || out == null || strategy == null) {
                throw new IllegalArgumentException(
                        "Usage: --events <path> --communities <path> " +
                                "--out <dir> --strategy <A|B|C|D|E|F|G|H> " +
                                "[--mode enrichment|detection]"
                );
            }
            return new Args(events, comm, out, mode, strategy);
        }
    }
}