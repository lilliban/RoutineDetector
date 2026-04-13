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
            case A -> Pattern.<EnrichedEvent>begin("start",skipStrategy)
                    .where(new SimpleCondition<EnrichedEvent>() {
                        @Override
                        public boolean filter(EnrichedEvent e){
                            return communityName.equals(e.community);
                        }
                    })
                    .times(activities.size())
                    .within(tMax);
            case B -> { //attualmente non ha finestre temporali attive
                //compaiono tutti gli eventi della community
                Pattern<EnrichedEvent, EnrichedEvent> pattern =
                        Pattern.<EnrichedEvent>begin("step_0", skipStrategy)
                                .where(new SimpleCondition<EnrichedEvent>() {
                                    @Override
                                    public boolean filter(EnrichedEvent e) {
                                        return e.activity.equals(activities.get(0));
                                    }
                                });

                for (int i = 1; i < activities.size(); i++) {
                    final int idx = i;
                    pattern = pattern.followedBy("step_" + idx)
                            .where(new SimpleCondition<EnrichedEvent>() {
                                @Override
                                public boolean filter(EnrichedEvent e) {
                                    return e.activity.equals(activities.get(idx));
                                }
                            });
                }
                yield pattern;
            }

            //scelto la strategia B..
            //condizioni A o B e permettiamo eventi di altre community nel mezzo
            case C -> Pattern.<EnrichedEvent>begin("start", skipStrategy)
                    .where(new SimpleCondition<EnrichedEvent>() {
                        @Override
                        public boolean filter(EnrichedEvent e){
                            return communityName.equals(e.community);
                        }
                    })
                    .times(activities.size())
                    .allowCombinations() //approssimazione perchè se volessimo precisione dovremmo codificare ogni evento con followedByAny
                    .within(tMax);

            //condizioni A o B e NON permettiamo eventi di altre community nel mezzo
            case D -> Pattern.<EnrichedEvent>begin("start", skipStrategy)
                    .where(new SimpleCondition<EnrichedEvent>() {
                        @Override
                        public boolean filter(EnrichedEvent e){
                            return communityName.equals(e.community);
                        }
                    })
                    .times(activities.size())
                    .consecutive()
                    .within(tMax);
            //compaiono un numero di eventi >= ad una % di eventi della community
            //come E considerando strategia A
            case E -> {
                int number = (int) (activities.size() * percent);
                yield Pattern.<EnrichedEvent>begin("start", skipStrategy)
                        .where(new SimpleCondition<EnrichedEvent>() {
                            @Override
                            public boolean filter(EnrichedEvent e){
                                return communityName.equals(e.community);
                            }
                        })
                        .times(number)
                        .within(tMax);
            }
            case F -> {
                int number = (int) (activities.size() * percent);
                yield Pattern.<EnrichedEvent>begin("start",skipStrategy)
                        .where(new SimpleCondition<EnrichedEvent>() {
                            @Override
                            public boolean filter(EnrichedEvent e){
                                return communityName.equals(e.community);
                            }
                        })
                        .times(number)
                        .within(tMax);
            }
            //come E considerando strategia B
            case G -> {
                int number = (int) (activities.size() * percent);
                yield Pattern.<EnrichedEvent>begin("start", skipStrategy)
                        .where(new SimpleCondition<EnrichedEvent>() {
                            @Override
                            public boolean filter(EnrichedEvent e){
                                return communityName.equals(e.community);
                            }
                        })
                        .times(number)
                        .consecutive()
                        .within(tMax);

            }
            //è passato un tempo T_max tra due eventi della community che danno origine a due attività distinte
            case H -> Pattern.<EnrichedEvent>begin("start", skipStrategy)
                    .where(new SimpleCondition<EnrichedEvent>() {
                        @Override
                        public boolean filter(EnrichedEvent e){
                            return communityName.equals(e.community);
                        }
                    })
                    .oneOrMore()
                    .consecutive()
                    .within(tMax);
        };

    }
}