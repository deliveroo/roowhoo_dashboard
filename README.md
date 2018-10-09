# Use case

provides a rolling 5 minute view of active consumers on a particular kafka cluster (provided they commit offsets to the __consumer_offsets topic)

# How to run it

You can build it using `sbt clean test stage`. Rough kubernetes deployment included (needs some further work)


# Notes

Kafka is regularly commiting messages to `__consumer_offsets`, even if no-one is consuming.

Still TODO
- configure internal topics properly to cater for things like
    greater fault tolerance by upping the replication factor
    tweaking retenton period
    
- support scaling instances of this streams application. maybe through using an akka 'load balancing' library like the one
used by order status application to correctly route requests for a particular key to the correct streams application instance.   

