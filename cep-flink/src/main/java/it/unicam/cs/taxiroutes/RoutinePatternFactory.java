package it.unicam.cs.taxiroutes;

import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import java.time.Duration;
import java.util.List;

public class RoutinePatternFactory {

    private static final AfterMatchSkipStrategy SKIP =
            AfterMatchSkipStrategy.skipPastLastEvent();

    public static Pattern<EnrichedEvent, ?> build(
            String communityName,
            Strategy strategy,
            List<String> activities,
            float percent,
            Duration tMax) {

        return switch (strategy) {

            case A -> Pattern.<EnrichedEvent>begin("start", SKIP)
                    .where(new SimpleCondition<EnrichedEvent>() {
                        @Override public boolean filter(EnrichedEvent e) {
                            return communityName.equals(e.community);
                        }
                    })
                    .times(activities.size())
                    .within(tMax);

            case B -> {
                Pattern<EnrichedEvent, EnrichedEvent> pattern =
                        Pattern.<EnrichedEvent>begin("step_0", SKIP)
                                .where(new SimpleCondition<EnrichedEvent>() {
                                    @Override public boolean filter(EnrichedEvent e) {
                                        return e.activity.equals(activities.get(0));
                                    }
                                });
                for (int i = 1; i < activities.size(); i++) {
                    final int idx = i;
                    pattern = pattern.followedBy("step_" + idx)
                            .where(new SimpleCondition<EnrichedEvent>() {
                                @Override public boolean filter(EnrichedEvent e) {
                                    return e.activity.equals(activities.get(idx));
                                }
                            });
                }
                yield pattern;
            }

            case C -> Pattern.<EnrichedEvent>begin("start", SKIP)
                    .where(new SimpleCondition<EnrichedEvent>() {
                        @Override public boolean filter(EnrichedEvent e) {
                            return communityName.equals(e.community);
                        }
                    })
                    .times(activities.size())
                    .allowCombinations()
                    .within(tMax);

            case D -> Pattern.<EnrichedEvent>begin("start", SKIP)
                    .where(new SimpleCondition<EnrichedEvent>() {
                        @Override public boolean filter(EnrichedEvent e) {
                            return communityName.equals(e.community);
                        }
                    })
                    .times(activities.size())
                    .consecutive()
                    .within(tMax);

            case E -> {
                int number = (int) (activities.size() * percent);
                yield Pattern.<EnrichedEvent>begin("start", SKIP)
                        .where(new SimpleCondition<EnrichedEvent>() {
                            @Override
                            public boolean filter(EnrichedEvent e) {
                                return communityName.equals(e.community);
                            }
                        })
                        .times(number)
                        .allowCombinations()
                        .within(tMax);
            }

            case F -> {
                int number = (int) (activities.size() * percent);
                yield Pattern.<EnrichedEvent>begin("start", SKIP)
                        .where(new SimpleCondition<EnrichedEvent>() {
                            @Override
                            public boolean filter(EnrichedEvent e) {
                                return communityName.equals(e.community);
                            }
                        })
                        .times(number)
                        .within(tMax);
            }

            case G -> {
                int number = (int) (activities.size() * percent);
                yield Pattern.<EnrichedEvent>begin("start", SKIP)
                        .where(new SimpleCondition<EnrichedEvent>() {
                            @Override public boolean filter(EnrichedEvent e) {
                                return communityName.equals(e.community);
                            }
                        })
                        .times(number)
                        .consecutive()
                        .within(tMax);
            }

            case H -> Pattern.<EnrichedEvent>begin("start", SKIP)
                    .where(new SimpleCondition<EnrichedEvent>() {
                        @Override public boolean filter(EnrichedEvent e) {
                            return communityName.equals(e.community);
                        }
                    })
                    .oneOrMore()
                    .consecutive()
                    .within(tMax);
        };
    }
}