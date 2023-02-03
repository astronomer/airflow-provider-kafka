ARG IMAGE_NAME="quay.io/astronomer/ap-airflow:2.4.3"
FROM ${IMAGE_NAME}

USER root
COPY airflow_provider_kafka ${AIRFLOW_HOME}/airflow_provider_kafka
COPY setup.cfg ${AIRFLOW_HOME}/airflow_provider_kafka/setup.cfg
COPY setup.py ${AIRFLOW_HOME}/airflow_provider_kafka/setup.py

RUN pip install -e ${AIRFLOW_HOME}/airflow_provider_kafka[dev]
USER astro
