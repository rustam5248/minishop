FROM eclipse-temurin:21-jre
WORKDIR /app
# OpenTelemetry Java agent: auto-instruments Spring MVC, Kafka clients, JDBC and HTTP clients.
ADD https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.11.0/opentelemetry-javaagent.jar /otel/opentelemetry-javaagent.jar
COPY target/app.jar app.jar
ENTRYPOINT ["java", "-javaagent:/otel/opentelemetry-javaagent.jar", "-jar", "app.jar"]
