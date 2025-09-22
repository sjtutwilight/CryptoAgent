FROM openjdk:11-jre-slim

WORKDIR /app

COPY target/aggregator-1.0-SNAPSHOT.jar /app/aggregator.jar
COPY src/main/resources/application-dev.properties /app/application-dev.properties

ENV JAVA_OPTS="--add-opens java.base/java.util=ALL-UNNAMED \
    --add-opens java.base/java.lang=ALL-UNNAMED \
    --add-opens java.base/java.lang.invoke=ALL-UNNAMED"

CMD ["sh", "-c", "java $JAVA_OPTS -jar aggregator.jar"] 