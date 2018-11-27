# Use case

provides a rolling 5 minute view of active consumers on a particular kafka cluster (provided they commit offsets to the __consumer_offsets topic)

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
