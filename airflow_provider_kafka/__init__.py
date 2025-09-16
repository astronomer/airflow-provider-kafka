# This is needed to allow Airflow to pick up specific metadata fields it needs
# for certain features. We recognize it's a bit unclean to define these in
# multiple places, but at this point it's the only workaround if you'd like your
# custom conn type to show up in the Airflow UI.
import sys
import warnings

__version__ = "0.2.5"


def get_provider_info():
    return {
        "package-name": "airflow-provider-kafka",  # Required
        "name": "Airflow Provider Kafka",  # Required
        "description": "Airflow hooks and operators for Kafka",  # Required
        "versions": [__version__],  # Required
    }



if not 'sdist' in sys.argv:
    warnings.warn("This package has been deprecated and incorporated into the apache airflow project. Please install via `pip install apache-airflow[apache.kafka]`", DeprecationWarning, stacklevel=2)
