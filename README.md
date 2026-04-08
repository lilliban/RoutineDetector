# RoutineDetector
1) mvn clean package -DskipTests
2) avviare docker compose
3) docker cp target\cep-flink-1.0-SNAPSHOT.jar jobmanager:/job.jar 
4) docker exec jobmanager flink run /job.jar `
  --events /data/input/nyc_taxi.csv `
--communities /data/communities/taxi_zone_lookup.csv `
  --out /result/ `
--mode detection `
--strategy A

