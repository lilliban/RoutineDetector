package it.unicam.routinedetector;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//.\run.bat --events ../data/input/EventLogXESNoSegment.csv --communities ../data/communities/communities_coms_infomap.txt --out ../result/ --mode detection --strategy A

public class Main {

    public static void main(String[] args) throws Exception {

        Args cfg = Args.parse(args);
        String subDir = cfg.outputDir + "/strategy-" + cfg.strategy;
        System.out.println("Events:      " + cfg.eventsPath);
        System.out.println("Communities: " + cfg.communitiesPath);
        System.out.println("Output dir:  " + cfg.outputDir);
        System.out.println("Strategy:    " + cfg.strategy); //Aggiunto strategy per la scelta della stategia
        System.out.println("Mode:        " + cfg.mode);

        // Load mapping activity -> community
        Map<String, String> activityToCommunity = CommunitiesParser.loadActivityToCommunity(cfg.communitiesPath);
        System.out.println("Loaded activity->community mappings: " + activityToCommunity.size());

        //aggiunto la chiamata alla nuova funzione creata, per ottenere il file delle comunità
        Map<String, List<String>> communities = CommunitiesParser.loadCommunities(cfg.communitiesPath);
        System.out.println("Loaded activity->community mappings: " + communities.size());
        for (Map.Entry<String, List<String>> entry : communities.entrySet()) {
            System.out.println("Community: " + entry.getKey() + " → " + entry.getValue().size() + " activities");
        }


        //Map<String, String> strategies = initializeStrategies();
        //System.out.println("Initialized detection strategies for " + strategies.size() + " communities");

        // Flink env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(cfg.eventsPath))
                .build();

        DataStream<String> lines = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "csv-source"
        );


        DataStream<Event> events = lines
                .map(new CsvEventParser())
                .name("parse-csv")
                .filter(e -> e != null)
                .name("drop-null")
                // vado a considerare solo eventi "complete"
                .filter(e -> "complete".equals(e.lifecycle))
                .name("keep-only-complete");

        /*
        DataStream<EnrichedEvent> enriched = events
                //vado a togliere il noise
                .filter(e -> !isNoise(e.activity))
                .name("filter-noise")
                .map(e -> {
                    String community = activityToCommunity.get(e.activity);
                    if (community == null) return null;
                    return new EnrichedEvent(e.timestamp, e.activity, community, e.eventId);
                })
                .name("enrich-community")
                .filter(x -> x != null)
                .name("drop-unknown");
        */

        //Ho provato a togliere il filtraggio del noise a monte..
        DataStream<EnrichedEvent> enriched = events
                .filter(e -> !isNoise(e.activity))
                .name("filter-noise")
                .map(e -> {
                    String community = activityToCommunity.getOrDefault(e.activity, "unknown");
                    return new EnrichedEvent(e.timestamp, e.activity, community, e.eventId);
                })
                .name("enrich-community");

        if ("enrichment".equalsIgnoreCase(cfg.mode)) {
            System.out.println("Running in ENRICHMENT mode (no CEP detection)");
            writeEnrichedEvents(enriched, cfg.outputDir);
        } else {
            System.out.println("Running in DETECTION mode (with CEP)");

            // watermarks per detection
            /*
            DataStream<EnrichedEvent> withWatermarks = enriched
                    .assignTimestampsAndWatermarks(
                            WatermarkStrategy
                                    .<EnrichedEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                    .withTimestampAssigner((event, timestamp) -> event.timestamp.toEpochMilli())
                    );
            */
            //il job finisce alla fine del file (prima asapettava 5 secondi)
            DataStream<EnrichedEvent> withWatermarks = enriched
                    .assignTimestampsAndWatermarks(
                            WatermarkStrategy
                                    .<EnrichedEvent>forMonotonousTimestamps()
                                    .withTimestampAssigner((event, timestamp) ->
                                            event.timestamp.toEpochMilli())
                    );


            for (Map.Entry<String, List<String>> entry : communities.entrySet()) {
                String community = entry.getKey();
                List<String> activities = entry.getValue();

                System.out.println("  - Detecting: " + community + " (strategy " + cfg.strategy + ")");
                DataStream<DetectedRoutine> detected = CepDetector.detectSimple(
                        withWatermarks,
                        community,
                        Strategy.valueOf(cfg.strategy),
                        activities,
                        0.8f,
                        Duration.ofMinutes(30)


                );


                writeDetectedRoutinesSeparate(detected, subDir, community);
            }
        }

        env.execute("RoutineDetector - " + cfg.mode);
        System.out.println("\nExecution completed! Check output in: " + cfg.outputDir);
        System.exit(0);
    }


    private static void writeEnrichedEvents(DataStream<EnrichedEvent> enriched, String outputDir) {
        OutputFileConfig fileCfg = OutputFileConfig
                .builder()
                .withPartPrefix("enriched_events")
                .withPartSuffix(".csv")
                .build();

        FileSink<String> sink = FileSink
                .forRowFormat(new Path(outputDir), new SimpleStringEncoder<String>("UTF-8"))
                .withOutputFileConfig(fileCfg)
                .build();

        enriched
                .map(EnrichedEvent::toCsvLine)
                .name("to-csv-line")
                .sinkTo(sink)
                .name("write-enriched");
    }


    private static void writeDetectedRoutines(DataStream<DetectedRoutine> routines, String outputDir) {
        OutputFileConfig fileCfg = OutputFileConfig
                .builder()
                .withPartPrefix("detected_routines")
                .withPartSuffix(".csv")
                .build();

        FileSink<String> sink = FileSink
                .forRowFormat(new Path(outputDir), new SimpleStringEncoder<String>("UTF-8"))
                .withOutputFileConfig(fileCfg)
                .build();

        routines
                .map(DetectedRoutine::toCsvLine)
                .name("to-csv-line")
                .sinkTo(sink)
                .name("write-detections");
    }


    private static Map<String, String> initializeStrategies() {
        Map<String, String> strategies = new HashMap<>();

        // Strategia A
        strategies.put("go_windows", "B");
        strategies.put("go_computer", "B");
        strategies.put("get_clothes", "B");
        strategies.put("go_chair", "B");
        strategies.put("go_tv", "B");
        strategies.put("go_oven", "B");
        strategies.put("get_cold_warm_food", "B");


        // Strategia C
        strategies.put("change_clothes", "C");
        strategies.put("get_water", "C");
        strategies.put("wc_do", "C");
        strategies.put("wash_hands", "C");
        strategies.put("go_dining_table", "C");

        // Strategia D
        strategies.put("interact_with_man", "D");
        strategies.put("do_exercise", "D");

        // Strategia BH (multi-step)
        strategies.put("go_bed", "BH");

        return strategies;
    }

    private static void writeDetectedRoutinesSeparate(
            DataStream<DetectedRoutine> routines,
            String outputDir,
            String community) {

        OutputFileConfig fileCfg = OutputFileConfig
                .builder()
                .withPartPrefix("Events_" + community)  // ← Nome file specifico
                .withPartSuffix(".csv")
                .build();

        FileSink<String> sink = FileSink
                .forRowFormat(new Path(outputDir), new SimpleStringEncoder<String>("UTF-8"))
                .withOutputFileConfig(fileCfg)
                .build();

        routines
                .map(DetectedRoutine::toCsvLine)
                .name("to-csv-" + community)
                .sinkTo(sink)
                .name("write-" + community);
    }

    private static boolean isNoise(String activity) {
        String a = activity.trim().toLowerCase();
        return a.startsWith("noise");
    }

    static class Args {
        final String eventsPath;
        final String communitiesPath;
        final String outputDir;
        final String mode;
        final String strategy;

        Args(String eventsPath, String communitiesPath, String outputDir, String mode, String strategy) {
            this.eventsPath = eventsPath;
            this.communitiesPath = communitiesPath;
            this.outputDir = outputDir;
            this.mode = mode;
            this.strategy = strategy;
        }

        static Args parse(String[] args) {
            String events = null, comm = null, out = null, mode = "detection", strategy = null;
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--events" -> events = args[++i];
                    case "--communities" -> comm = args[++i];
                    case "--out" -> out = args[++i];
                    case "--strategy" -> strategy = args[++i]; //Aggiunto strategy per la scelta della stategia
                    case "--mode" -> mode = args[++i];

                }
            }
            if (events == null || comm == null || out == null || strategy == null) { //Aggiunto strategy per la scelta della stategia
                throw new IllegalArgumentException("Usage: --events <path> --communities <path> --out <dir> [--mode enrichment|detection]");
            }
            return new Args(events, comm, out, mode, strategy);
        }
    }
}