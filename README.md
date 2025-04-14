# kafka-error-example

This is a simplistic application to test spring-kafka's handling of Java `Error`s

## Setup

This application assumes you have Kafka running. It is assumed to be running on `localhost:9092`
but if this is not the case it can be changed in `src\main\resources\application.yaml` by modifying
`kafka.bootstrap`

## Running

```
mvn spring-boot:run
```

For convienence sake [Swagger](http://localhost:8080/swagger-ui/index.html) is setup at `localhost:8080/swagger-ui/index.html` which
has a single endpoint for sending requests to the topic that has the listener setup to throw an error.