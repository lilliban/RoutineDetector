package it.unicam.routinedetector;

import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import it.unicam.routinedetector.EnrichedEvent;
import it.unicam.routinedetector.Strategy;


public class RoutinePatternFactory {



    //fatto
    public static Pattern<EnrichedEvent, ?> changeClothes() {
        return Pattern.<EnrichedEvent>begin("start")
                .where(new SimpleCondition<EnrichedEvent>() {
                    @Override
                    public boolean filter(EnrichedEvent e) {
                        return "change_clothes".equals(e.community);
                    }
                })
                .times(4)
                .consecutive()
                .greedy()
                .within(Duration.ofHours(10)); // ci sta work che dura !10 ore!
    }

    //  28-35
    // decidere se mettere anche il .consecutive
    public static Pattern<EnrichedEvent, ?> getWater() {
        return Pattern.<EnrichedEvent>begin("start")
                .where(new SimpleCondition<EnrichedEvent>() {
                    @Override
                    public boolean filter(EnrichedEvent e) {
                        return "get_water".equals(e.community);
                    }
                })
                .times(5)
                .consecutive()
                .greedy()
                .within(Duration.ofMinutes(15));

    }


    public static Pattern<EnrichedEvent, ?> wcDo() {
        return Pattern.<EnrichedEvent>begin("start")
                .where(new SimpleCondition<EnrichedEvent>() {
                    @Override
                    public boolean filter(EnrichedEvent e) {
                        return "wc_do".equals(e.community);
                    }
                })
                .times(4)
                .consecutive()
                .greedy()
                .within(Duration.ofMinutes(15));
    }


    //260
    public static Pattern<EnrichedEvent, ?> washHands() {
        return Pattern.<EnrichedEvent>begin("start")
                .where(new SimpleCondition<EnrichedEvent>() {
                    @Override
                    public boolean filter(EnrichedEvent e)  {
                        return "wash_hands".equals(e.community);
                    }
                })
                .oneOrMore()
                .consecutive()
                .greedy()
                .within(Duration.ofMinutes(10));
    }





    //879
    // con allow combination fai passare la sedia in mezzo
    public static Pattern<EnrichedEvent, ?> goWindows() {
        return Pattern.<EnrichedEvent>begin("start")
                .where(new SimpleCondition<EnrichedEvent>() {
                    @Override
                    public boolean filter(EnrichedEvent e)  {
                        return "go_windows".equals(e.community);
                    }
                })
                .timesOrMore(2)
                .consecutive()
                .greedy()
                .within(Duration.ofMinutes(15));
    }


    // 16

    public static Pattern<EnrichedEvent, ?> goBed() {
        return Pattern.<EnrichedEvent>begin("brush")
                .where(new SimpleCondition<EnrichedEvent>() {
                    @Override
                    public boolean filter(EnrichedEvent e) {
                        return "brush_teeth".equals(e.activity);
                    }
                })
                .optional()  // non sempre si lava i denti
                .followedByAny("andare_letto")
                .where(new SimpleCondition<EnrichedEvent>() {
                    @Override
                    public boolean filter(EnrichedEvent e) {
                        return "go_bed".equals(e.activity);
                    }
                })
                .followedByAny("dormire")
                .where(new SimpleCondition<EnrichedEvent>() {
                    @Override
                    public boolean filter(EnrichedEvent e) {
                        return "sleep_in_bed".equals(e.activity);
                    }
                })
                .followedByAny("svegliarsi")
                .where(new SimpleCondition<EnrichedEvent>() {
                    @Override
                    public boolean filter(EnrichedEvent e) {
                        return "go_to_start".equals(e.activity);
                    }
                })
                .optional()  // non ci sta sempre
                .within(Duration.ofHours(3));
    }
        //    Pattern.<EnrichedEvent>begin("start")
        //    .where(e -> "go_bed".equals(e.community))
        //    .timesOrMore(2)
        //    .consecutive()
        //    .greedy()
        //    .within(Duration.ofHours(3));


    public static Pattern<EnrichedEvent, ?> goComputer() {
        return Pattern.<EnrichedEvent>begin("start")
                .where(new SimpleCondition<EnrichedEvent>() {
                    @Override
                    public boolean filter(EnrichedEvent e) {
                        return "go_computer".equals(e.community);
                    }
                })
                .times(4)
                .consecutive()
                .greedy()
                .within(Duration.ofHours(1));
    }




    //1751
    // .timesOrMore(2) differenza con onoremore
    public static Pattern<EnrichedEvent, ?> goDiningTable() {
        return Pattern.<EnrichedEvent>begin("start")
                .where(new SimpleCondition<EnrichedEvent>() {
                    @Override
                    public boolean filter(EnrichedEvent event) {
                        return "go_dining_table".equals(event.community);
                    }
                })
                .timesOrMore(2)
                .consecutive()
                .greedy()
                .within(Duration.ofMinutes(30));
    }




    public static Pattern<EnrichedEvent, ?> getClothes() {
        return Pattern.<EnrichedEvent>begin("start")
                .where(new SimpleCondition<EnrichedEvent>() {
                    @Override
                    public boolean filter(EnrichedEvent event) {
                        return "get_clothes".equals(event.community);
                    }
                })
                .timesOrMore(2)
                .consecutive()
                .greedy()
                .within(Duration.ofMinutes(45));
    }



//stanno una dopo l'altra escluso il noise che dovrebbe essere filtrato
    public static Pattern<EnrichedEvent, ?> interactWithMan() {
        return Pattern.<EnrichedEvent>begin("start")
                .where(new SimpleCondition<EnrichedEvent>() {
                    @Override
                    public boolean filter(EnrichedEvent event) {
                        return "interact_with_man".equals(event.community);
                    }
                })
                .timesOrMore(2)
                .consecutive()
                .greedy()
                .within(Duration.ofMinutes(10));
    }

    public static Pattern<EnrichedEvent, ?> goChair() {
        return Pattern.<EnrichedEvent>begin("start")
                .where(new SimpleCondition<EnrichedEvent>() {
                    @Override
                    public boolean filter(EnrichedEvent event) {
                        return "go_chair".equals(event.community);
                    }
                })
                .times(2)
                .consecutive()
                .greedy()
                .within(Duration.ofMinutes(30));
    }

    public static Pattern<EnrichedEvent, ?> doExercise() {
        return Pattern.<EnrichedEvent>begin("start")
                .where(new SimpleCondition<EnrichedEvent>() {
                    @Override
                    public boolean filter(EnrichedEvent event) {
                        return "do_exercise".equals(event.community);
                    }
                })
                .timesOrMore(2)
                .consecutive()
                .greedy()
                .within(Duration.ofMinutes(60));
    }


    public static Pattern<EnrichedEvent, ?> goTv() {
        return Pattern.<EnrichedEvent>begin("start")
                .where(new SimpleCondition<EnrichedEvent>() {
                    @Override
                    public boolean filter(EnrichedEvent e) {
                        return "go_tv".equals(e.community);
                    }
                })
                .timesOrMore(2)
                .consecutive()
                .greedy()
                .within(Duration.ofMinutes(60));
    }

    public static Pattern<EnrichedEvent, ?> goOven() {
        return Pattern.<EnrichedEvent>begin("start")
                .where(new SimpleCondition<EnrichedEvent>() {
                    @Override
                    public boolean filter(EnrichedEvent e) {
                        return "go_oven".equals(e.community);
                    }
                })
                .timesOrMore(2)
                .consecutive()
                .greedy()
                .within(Duration.ofMinutes(45));
    }

    public static Pattern<EnrichedEvent, ?> getColdWarmFood() {
        return Pattern.<EnrichedEvent>begin("start")
                .where(new SimpleCondition<EnrichedEvent>() {
                    @Override
                    public boolean filter(EnrichedEvent e) {
                        return "get_cold_warm_food".equals(e.community);
                    }
                })
                .timesOrMore(2)
                .consecutive()
                .greedy()
                .within(Duration.ofMinutes(15));
    }

    public static AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
    public static Pattern<EnrichedEvent, ?> build(
            String communityName,
            Strategy strategy,
            List<String> activities,
            float percent,
            Duration tMax) {
        return switch (strategy) {
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

            case E, F -> {
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