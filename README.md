# Kafka Airflow Provider



## Testing
### Unit Tests

Unit tests are located at `tests/unit`, a kafka server isn't required to run these tests.
execute with `pytest`

## Development 

```
pip install .[dev]
```
You can bring up the development environment with `make dev` this will spin up a complete environment (including kafka) via `docker-compose` and load the DAGs residing in the `example_dags` folder.


## Setup on M1 Mac
Installing on M1 chip means a brew install of the librdkafka library
```bash
brew install librdkafka
export C_INCLUDE_PATH=/opt/homebrew/Cellar/librdkafka/1.8.2/include
export LIBRARY_PATH=/opt/homebrew/Cellar/librdkafka/1.8.2/lib
pip install confluent-kafka
```
