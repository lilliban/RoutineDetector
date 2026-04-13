package it.unicam.routinedetector;

import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import java.time.Duration;
import java.util.List;

public class RoutinePatternFactory {

    public static AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
    public static Pattern<EnrichedEvent, ?> build(
            String communityName,
            Strategy strategy,
            List<String> activities,
            float percent,
            Duration tMax) {
        return switch (strategy) {
            
            //compaiono tutti gli eventi della community
            case A -> {
                Pattern<EnrichedEvent, EnrichedEvent> pattern =
                        Pattern.<EnrichedEvent>begin("step_0", skipStrategy)
                                .where(new SimpleCondition<EnrichedEvent>() {
                                    @Override
                                    public boolean filter(EnrichedEvent e) {
                                        return e.community.equals(communityName) && e.activity.equals(activities.get(0));
                                    }
                                });
                for (int i = 1; i < activities.size(); i++) {
                    final int idx = i;
                    pattern = pattern.followedByAny("step_" + idx)
                            .where(new SimpleCondition<EnrichedEvent>() {
                                @Override
                                public boolean filter(EnrichedEvent e) {
                                    return e.community.equals(communityName) && e.activity.equals(activities.get(idx));
                                }
                            });
                }
                yield pattern;
            }

            //compaiono tutti gli eventi della community nell'ordine riportati nel pattern della community
            case B -> {
                Pattern<EnrichedEvent, EnrichedEvent> pattern =
                        Pattern.<EnrichedEvent>begin("step_0", skipStrategy)
                                .where(new SimpleCondition<EnrichedEvent>() {
                                    @Override
                                    public boolean filter(EnrichedEvent e) {
                                        return e.community.equals(communityName) && e.activity.equals(activities.get(0));
                                    }
                                });
                for (int i = 1; i < activities.size(); i++) {
                    final int idx = i;
                    pattern = pattern.followedBy("step_" + idx)
                            .where(new SimpleCondition<EnrichedEvent>() {
                                @Override
                                public boolean filter(EnrichedEvent e) {
                                    return e.community.equals(communityName) && e.activity.equals(activities.get(idx));
                                }
                            });
                }
                yield pattern;
            }

            //condizioni A o B e permettiamo eventi di altre community nel mezzo
            case C -> Pattern.<EnrichedEvent>begin("start", skipStrategy)
                    .where(new SimpleCondition<EnrichedEvent>() {
                        @Override
                        public boolean filter(EnrichedEvent e) {
                            return communityName.equals(e.community);
                        }
                    })
                    .times(activities.size())
                    .allowCombinations();

            //condizioni A o B e NON permettiamo eventi di altre community nel mezzo
            case D -> Pattern.<EnrichedEvent>begin("start", skipStrategy)
                    .where(new SimpleCondition<EnrichedEvent>() {
                        @Override
                        public boolean filter(EnrichedEvent e) {
                            return communityName.equals(e.community);
                        }
                    })
                    .times(activities.size())
                    .consecutive();

            //compaiono un numero di eventi >= ad una % degli eventi della community (nessun vincolo di ordine)
            case E -> {
                int number = (int) Math.ceil(activities.size() * percent);
                yield Pattern.<EnrichedEvent>begin("start", skipStrategy)
                        .where(new SimpleCondition<EnrichedEvent>() {
                            @Override
                            public boolean filter(EnrichedEvent e) {
                                return communityName.equals(e.community);
                            }
                        })
                        .timesOrMore(number);
            }

            //come E considerando strategia A (qualsiasi ordine, eventi di altre community ammessi)
            case F -> {
                int number = (int) Math.ceil(activities.size() * percent);
                yield Pattern.<EnrichedEvent>begin("start", skipStrategy)
                        .where(new SimpleCondition<EnrichedEvent>() {
                            @Override
                            public boolean filter(EnrichedEvent e) {
                                return communityName.equals(e.community);
                            }
                        })
                        .timesOrMore(number)
                        .allowCombinations();
            }

            //come E considerando strategia B (ordine specifico del pattern)
            case G -> {
                int number = (int) Math.ceil(activities.size() * percent);
                Pattern<EnrichedEvent, EnrichedEvent> pattern =
                        Pattern.<EnrichedEvent>begin("step_0", skipStrategy)
                                .where(new SimpleCondition<EnrichedEvent>() {
                                    @Override
                                    public boolean filter(EnrichedEvent e) {
                                        return e.community.equals(communityName) && e.activity.equals(activities.get(0));
                                    }
                                });
                for (int i = 1; i < number; i++) {
                    final int idx = i;
                    pattern = pattern.followedBy("step_" + idx)
                            .where(new SimpleCondition<EnrichedEvent>() {
                                @Override
                                public boolean filter(EnrichedEvent e) {
                                    return e.community.equals(communityName) && e.activity.equals(activities.get(idx));
                                }
                            });
                }
                yield pattern;
            }

            //e' passato un tempo >= T_max tra due eventi della community (gap = due attivita' distinte)
            case H -> Pattern.<EnrichedEvent>begin("before_gap", skipStrategy)
                    .where(new SimpleCondition<EnrichedEvent>() {
                        @Override
                        public boolean filter(EnrichedEvent e) {
                            return communityName.equals(e.community);
                        }
                    })
                    .notFollowedBy("during_gap")
                    .where(new SimpleCondition<EnrichedEvent>() {
                        @Override
                        public boolean filter(EnrichedEvent e) {
                            return communityName.equals(e.community);
                        }
                    })
                    .within(tMax)
                    .followedBy("after_gap")
                    .where(new SimpleCondition<EnrichedEvent>() {
                        @Override
                        public boolean filter(EnrichedEvent e) {
                            return communityName.equals(e.community);
                        }
                    });
        };

    }
}