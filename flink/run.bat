@echo off
java --add-opens=java.base/java.util=ALL-UNNAMED ^
     --add-opens=java.base/java.util.concurrent=ALL-UNNAMED ^
     --add-opens=java.base/java.lang=ALL-UNNAMED ^
     --add-opens=java.base/java.lang.invoke=ALL-UNNAMED ^
     --add-opens=java.base/java.lang.reflect=ALL-UNNAMED ^
     --add-opens=java.base/java.io=ALL-UNNAMED ^
     --add-opens=java.base/sun.nio.ch=ALL-UNNAMED ^
     -jar target/routine-detector-flink-1.0-SNAPSHOT.jar %*