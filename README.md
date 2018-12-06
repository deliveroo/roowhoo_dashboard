# What does this solve?
This app gives up to date information about kafka consuming groups that are live and consuming from a kafka cluster. It exposes the Group Id and Client Id as well as providing information on the live instances and which partitions are assigned to each instance.


# How to run it locally

Run an instance of kafka locally, or port-forward to an instance running in kubernetes.

```bash
./run-locally.sh --kafka_username=<USERNAME> \
                 --kafka_password=<PASSWORD> \
                 --broker_bootstrap=localhost:9093 \
                 --broker_protocol=SASL_SSL \
                 --zookeeper_host=localhost \
                 --zookeeper_port=2181
```

View at http://localhost:9000
This will run a play application, and consume from a kafka internal topic, `__consumer_offsets`. It can take a little while for the app to collect data, so you may see a loading page for the first time you execute the app.


# How does it work?
This app is making use of the kafka streams API to process the  `__consumer_offsets` topic, and stores this in a local DB on your machine. 


