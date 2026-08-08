# =============================================================================
# Apache Flink Job Docker Image
# =============================================================================
# Base image for both JobManager and TaskManager containers in docker-compose.
# The same image is used with different CMD overrides:
#   - jobmanager  -> coordinates job scheduling and checkpointing
#   - taskmanager -> executes task slots and runs operators
#
# This image also bundles:
#   - Python 3 + PyFlink 1.18.1 (for submitting the PyFlink job)
#   - flink-sql-connector-kafka JAR (Kafka source/sink connector)
#   - job.py copied to /opt/flink/usrlib/ for submission
#
# To submit the job after stack is up:
#   docker exec -it jobmanager ./bin/flink run -py /opt/flink/usrlib/job.py
# =============================================================================

FROM flink:1.18.1-scala_2.12

# Install Python runtime and dev headers (needed for PyFlink native bindings)
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# PyFlink version must match the Flink base image version (1.18.1)
RUN pip3 install --no-cache-dir apache-flink==1.18.1

# Kafka connector JAR — version 3.1.0-1.18 is built for Flink 1.18.x
# Placed in /opt/flink/lib/ so the Flink classloader picks it up automatically
RUN wget -q -P /opt/flink/lib/ \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

# Application job script; submit with: flink run -py /opt/flink/usrlib/job.py
COPY job.py /opt/flink/usrlib/job.py
