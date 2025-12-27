FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

COPY target/aggregator-1.0-SNAPSHOT.jar /app/aggregator.jar
COPY src/main/resources/application-container.properties /app/application-container.properties

ENV JAVA_OPTS="--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"

CMD ["sh", "-c", "java $JAVA_OPTS -Denv=container -jar aggregator.jar"]
