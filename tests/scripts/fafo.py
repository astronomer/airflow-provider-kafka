from distutils.command.config import config
from socket import timeout
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer, Consumer
from asgiref.sync import sync_to_async


import json


def acked(err, msg):
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))


TOPICS = ["numbers_1","numbers_2"]

# ADMIN CLIENT 
config = {
    'bootstrap.servers' : 'localhost:9092'
}

admin = AdminClient(config)

new_topics = [NewTopic(topic,num_partitions=3,replication_factor=1) for topic in TOPICS]

futures = admin.create_topics(new_topics)

for t,f in futures.items():
    try:
        f.result()
        print(f"Topic {t} created.")
    except Exception as e:
        # if e.args[0].get('code',None) == 'TOPIC_ALREADY_EXISTS':
        #     pass
        
        print(e.args[0].name())



producer = Producer(config)

for t in TOPICS:
    for x in range(1000):
        producer.produce(t, key=f"{t}_{x}", value=json.dumps({"count":x}), on_delivery=acked)
    producer.flush()


extra_config = {
    "group.id" : "foo",
    "enable.auto.commit":False,
    "auto.offset.reset": "beginning"
}

consumer = Consumer({**config, **extra_config})

consumer.subscribe(["numbers_1"])

msgs = consumer.consume(num_messages = 100, timeout=60)
print(msgs)
for m in msgs:
    try:
        x = (m.topic(), 
              m.partition(), 
              m.key(), 
              json.loads(m.value())
              )
    except Exception as e:
        print(f"{ e }")

consumer.commit()
consumer.close()

### multiple_topics
extra_config = {
    "group.id" : "foo2",
    "enable.auto.commit":False,
    "auto.offset.reset": "beginning"
}
consumer2 = Consumer({**config, **extra_config})
consumer2.subscribe(['^.*$'])
msgs = consumer2.consume(num_messages = 1000, timeout=60)
for m in msgs:
    try:
        no_op = (m.topic(), 
              m.partition(), 
              m.key(), 
              json.loads(m.value()))          
    except Exception as e:
        print(f"{ e }")

consumer2.commit()
consumer2.close()