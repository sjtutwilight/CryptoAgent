FROM confluentinc/cp-kafka:7.4.1

USER root

ARG JMX_EXPORTER_VERSION=0.20.0
RUN mkdir -p /opt/jmx-exporter \
    && curl -fsSL "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar" \
        -o /opt/jmx-exporter/jmx_prometheus_javaagent.jar \
    && chown -R appuser:appuser /opt/jmx-exporter

COPY jmx/kafka-broker.yml /opt/jmx-exporter/config.yml
RUN chown appuser:appuser /opt/jmx-exporter/config.yml

USER appuser
