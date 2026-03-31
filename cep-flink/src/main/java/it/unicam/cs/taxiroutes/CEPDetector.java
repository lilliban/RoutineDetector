package it.unicam.cs.taxiroutes;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class CEPDetector {

    public static DataStream<DetectedRoutine> detect(
            KeyedStream<EnrichedEvent, String> keyed,
            Pattern<EnrichedEvent, ?> pattern,
            String routineName,
            String strategy) {

        PatternStream<EnrichedEvent> patternStream = CEP.pattern(keyed, pattern);

        return patternStream.process(new PatternProcessFunction<EnrichedEvent, DetectedRoutine>() {
            @Override
            public void processMatch(
                    Map<String, List<EnrichedEvent>> match,
                    Context ctx,
                    Collector<DetectedRoutine> out) {

                List<EnrichedEvent> allEvents = new ArrayList<>();
                for (List<EnrichedEvent> stepEvents : match.values()) {
                    if (stepEvents != null) allEvents.addAll(stepEvents);
                }
                if (allEvents.isEmpty()) return;

                allEvents.sort(Comparator.comparing(e -> e.timestamp));

                long startTime = allEvents.get(0).timestamp.toEpochMilli();
                long endTime   = allEvents.get(allEvents.size() - 1).timestamp.toEpochMilli();

                List<String> activities = new ArrayList<>();
                for (EnrichedEvent e : allEvents) activities.add(e.activity);

                String caseId = allEvents.get(0).caseId;

                out.collect(new DetectedRoutine(
                        routineName, strategy, caseId,
                        startTime, endTime, activities
                ));
            }
        });
    }

    public static DataStream<DetectedRoutine> detectSimple(
            DataStream<EnrichedEvent> stream,
            String community,
            Strategy strategy,
            List<String> activities,
            float percent,
            Duration tMax) {

        KeyedStream<EnrichedEvent, String> keyed = stream.keyBy(e -> e.caseId);

        Pattern<EnrichedEvent, ?> pattern = RoutinePatternFactory.build(
                community, strategy, activities, percent, tMax);

        return detect(keyed, pattern, community, strategy.name());
    }
}
