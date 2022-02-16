ARG IMAGE_NAME="quay.io/astronomer/ap-airflow:2.2.3"
FROM ${IMAGE_NAME}

USER root
COPY kafka_provider ${AIRFLOW_HOME}/kafka_provider
COPY setup.cfg ${AIRFLOW_HOME}/kafka_provider/setup.cfg
COPY setup.py ${AIRFLOW_HOME}/kafka_provider/setup.py

RUN pip install  ${AIRFLOW_HOME}/kafka_provider[dev]
USER astro
