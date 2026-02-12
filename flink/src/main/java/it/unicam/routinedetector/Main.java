package it.unicam.routinedetector;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig; // ← QUI!

import java.time.Duration;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws Exception {

        Args cfg = Args.parse(args);

        System.out.println("Events:      " + cfg.eventsPath);
        System.out.println("Communities: " + cfg.communitiesPath);
        System.out.println("Output dir:  " + cfg.outputDir);

        // 1) Load mapping activity -> community
        //
        Map<String, String> activityToCommunity = CommunitiesParser.loadActivityToCommunity(cfg.communitiesPath);
        System.out.println("Loaded activity->community mappings: " + activityToCommunity.size());

        // 2) Flink env
        //ho un solo task in parallelo
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 3) Leggi il CSV come righe di testo
        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(cfg.eventsPath))
                .build();
        //creo uno stream di dati i cui elemnti sono String
        //DA SISTEMARE IL WATERMARKING
        DataStream<String> lines = env.fromSource(source, WatermarkStrategy.noWatermarks(), "csv-source");

        // 4) Parse CSV -> Event
        DataStream<Event> events = lines
                .map(new CsvEventParser())
                .name("parse-csv")
                .filter(e -> e != null)
                .name("drop-null");

        // 5) Filter noise + enrich with community (drop unknown)
        DataStream<EnrichedEvent> enriched = events
                .filter(e -> !isNoise(e.activity))
                .name("filter-noise")
                .map(e -> {
                    String community = activityToCommunity.get(e.activity);
                    if (community == null) return null; // unknown -> drop
                    return new EnrichedEvent(e.timestamp, e.activity, community, e.eventId);
                })
                .name("enrich-community")
                .filter(x -> x != null)
                .name("drop-unknown");

        // 6) Sink: write enriched events to output folder (nome file sensato)
        // 6) Sink: write enriched events to output folder
        OutputFileConfig fileCfg = OutputFileConfig
                .builder()
                .withPartPrefix("enriched_events")
                .withPartSuffix(".csv")
                .build();

        FileSink<String> sink = FileSink
                .forRowFormat(new Path(cfg.outputDir), new SimpleStringEncoder<String>("UTF-8"))
                .withOutputFileConfig(fileCfg)
                .build();

        enriched
                .map(EnrichedEvent::toCsvLine)
                .name("to-csv-line")
                .sinkTo(sink)
                .name("write-output");


        env.execute("RoutineDetector - Baseline Enrichment");
    }

    private static boolean isNoise(String activity) {
        // gestisce "noise 20", "noise_20", "noise" ecc.
        String a = activity.trim().toLowerCase();
        return a.startsWith("noise");
    }

    // config minimale
    static class Args {
        final String eventsPath;
        final String communitiesPath;
        final String outputDir;

        Args(String eventsPath, String communitiesPath, String outputDir) {
            this.eventsPath = eventsPath;
            this.communitiesPath = communitiesPath;
            this.outputDir = outputDir;
        }

        static Args parse(String[] args) {
            String events = null, comm = null, out = null;
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--events" -> events = args[++i];
                    case "--communities" -> comm = args[++i];
                    case "--out" -> out = args[++i];
                }
            }
            if (events == null || comm == null || out == null) {
                throw new IllegalArgumentException("Usage: --events <path> --communities <path> --out <dir>");
            }
            return new Args(events, comm, out);
        }
    }
}