## Jaeger Hot Rod

This demo will use the Jaeger Hotrod app. 

#### Required:
* Docker - If you run Docker locally, we recommend allowing Docker to use at least 4 GB of RAM in Preferences > Resources.

#### Demo
```
docker-compose up -d --build
``` 

The above command will start the Jaeger Hotrod sample, Jaeger Agent, OpenTelemetry Collector, SITUP, ODFE and Kibana.

After successful start, use the hot rod app which runs at localhost:8080. Now check the traces in kibana which runs at localhost:5601.

You will see two indicies,

* otel-v1-apm-span  - This index alias will store the raw traces.
* otel-v1-apm-service-map - This index will store the relationship between services.


