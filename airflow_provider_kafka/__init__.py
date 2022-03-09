# This is needed to allow Airflow to pick up specific metadata fields it needs
# for certain features. We recognize it's a bit unclean to define these in
# multiple places, but at this point it's the only workaround if you'd like your
# custom conn type to show up in the Airflow UI.

__version__= "0.1.0"
def get_provider_info():
    return {
        "package-name": "airflow-provider-kafka",  # Required
        "name": "Airflow Provider Kafka",  # Required
        "description": "Airflow hooks and operators for Kafka",  # Required
        "versions": [__version__],  # Required
    }



