

## Setup on M1 Mac
Installing on M1 chip
```bash
git clone https://github.com/edenhill/librdkafka.git
cd librdkafka
brew install openssl zstd pkg-config
export CPPFLAGS="-I/opt/homebrew/opt/openssl@1.1/include"
export LDFLAGS="-L/opt/homebrew/opt/openssl@1.1/lib"
./configure
make
sudo make install
pip install confluent-kafka
```