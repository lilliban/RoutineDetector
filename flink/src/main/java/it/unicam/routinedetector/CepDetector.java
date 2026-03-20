package it.unicam.routinedetector;

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

//va a prendere uno stream di EnrichedEvent epplica il CEP e produce uno stream di DetectedRoutine
public class CepDetector {


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
                    if (stepEvents != null) {
                        allEvents.addAll(stepEvents);
                    }
                }


                if (allEvents.isEmpty()) return;


                // ordino per timestamp, perché gli eventi possono arrivare fuori ordine
                allEvents.sort(Comparator.comparing(e -> e.timestamp));

                // Calcola start/end dalla sequenza ordinata
                long startTime = allEvents.get(0).timestamp.toEpochMilli();
                long endTime   = allEvents.get(allEvents.size() - 1).timestamp.toEpochMilli();

                // Estrai lista attività in ordine cronologico
                List<String> activities = new ArrayList<>();
                for (EnrichedEvent e : allEvents) {
                    activities.add(e.activity);
                }


                String caseId ="house_01";

                DetectedRoutine routine = new DetectedRoutine(
                        routineName,
                        strategy,
                        caseId,
                        startTime,
                        endTime,
                        activities
                );

                out.collect(routine);
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

        //reggruppi per community
        KeyedStream<EnrichedEvent, String> keyed = stream.keyBy(e -> "house_01");

        Pattern<EnrichedEvent, ?> pattern = RoutinePatternFactory.build(community, strategy, activities, percent, tMax);
        /*
        Pattern<EnrichedEvent, ?> pattern = switch (community) {
            case "go_tv"             -> RoutinePatternFactory.goTv();
            case "wc_do"             -> RoutinePatternFactory.wcDo();
            case "go_bed"            -> RoutinePatternFactory.goBed();
            case "go_computer"       -> RoutinePatternFactory.goComputer();
            case "change_clothes"    -> RoutinePatternFactory.changeClothes();
            case "get_water"         -> RoutinePatternFactory.getWater();
            case "wash_hands"        -> RoutinePatternFactory.washHands();
            case "go_windows"        -> RoutinePatternFactory.goWindows();
            case "go_oven"           -> RoutinePatternFactory.goOven();
            case "get_clothes"       -> RoutinePatternFactory.getClothes();
            case "get_cold_warm_food"-> RoutinePatternFactory.getColdWarmFood();
            case "interact_with_man" -> RoutinePatternFactory.interactWithMan();
            case "do_exercise"       -> RoutinePatternFactory.doExercise();
            case "go_chair"          -> RoutinePatternFactory.goChair();
            case "go_dining_table"   -> RoutinePatternFactory.goDiningTable();
            default -> throw new IllegalArgumentException("Unknown community: " + community);
          */


        return detect(keyed, pattern, community, strategy.name());
    }
}