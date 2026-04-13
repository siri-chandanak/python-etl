FROM apache/spark:3.5.3-python3

USER root

WORKDIR /opt/spark/jobs

COPY jobs/ /opt/spark/jobs/

ADD https://repo1.maven.org/maven2/org/apache/gravitino/gravitino-spark-connector-runtime-3.5_2.12/1.2.0/gravitino-spark-connector-runtime-3.5_2.12-1.2.0.jar /opt/spark/jars/

# 🔥 FIX EVERYTHING
RUN chmod -R 755 /opt/spark/jobs \
 && chmod -R 755 /opt/spark/jars \
 && chown -R 185:185 /opt/spark/jobs \
 && chown -R 185:185 /opt/spark/jars

USER 185