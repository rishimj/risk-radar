# PyFlink publishes NO linux/aarch64 wheel at any version (checked PyPI 1.18 -> 2.3:
# manylinux x86_64 and macosx_11_0_arm64 only), so this image is pinned to amd64
# and runs under Rosetta on an M-series Mac. On the x86_64 EC2 target it is native.
#
# 1.20.1 rather than 1.18: newest 1.x with the DataStream Python API intact (2.x
# reworks it), and its wheels cover cp38-cp311, which matches this base image's
# Debian bookworm python3.11.
FROM --platform=linux/amd64 flink:1.20.1-scala_2.12-java11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 python3-pip python3-dev build-essential curl \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# Kafka connector on the CLUSTER classpath rather than per-job env.add_jars():
# the JobManager, TaskManager and the submitter all run this image, so one copy
# in /opt/flink/lib serves all three.
ARG KAFKA_CONNECTOR=flink-sql-connector-kafka-3.3.0-1.20.jar
RUN curl -fsSL -o /opt/flink/lib/${KAFKA_CONNECTOR} \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/${KAFKA_CONNECTOR}

RUN pip3 install --no-cache-dir --break-system-packages \
      apache-flink==1.20.1 \
      redis>=5.0 \
      boto3>=1.34 \
      requests>=2.31

# riskcore must be present on the TASKMANAGER too, not just the submitter — the
# old repo ran stock flink images for JM/TM, so any Python UDF would have found
# no domain code on the worker.
COPY libs/core /libs/core
RUN pip3 install --no-cache-dir --break-system-packages /libs/core

COPY flink/job /opt/riskradar/job

ENV PYTHONUNBUFFERED=1
