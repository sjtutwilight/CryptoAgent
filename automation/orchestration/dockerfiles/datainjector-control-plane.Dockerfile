# syntax=docker/dockerfile:1

FROM maven:3.9.6-eclipse-temurin-17 AS builder
WORKDIR /workspace
COPY datainjector/control-plane-service/pom.xml .
COPY datainjector/control-plane-service/src ./src
RUN mvn -f pom.xml clean package -DskipTests
RUN JAR_FILE=$(ls target/*.jar | head -n 1) && mv "$JAR_FILE" app.jar

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=builder /workspace/app.jar app.jar
EXPOSE 8083
ENTRYPOINT ["java","-jar","/app/app.jar"]
