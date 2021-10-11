## Jaeger Hot Rod

This demo will use the revered Jaeger Hotrod app. The purpose of this demo is to experience the Trace Analytics feature using an existing Jaeger based application. For this demo we use the Jager Hotrod App, Jaeger Agent, OpenTelemetry collector, OpenSearch and Data Prepper.

#### Required:
* Docker - we recommend allowing Docker to use at least 4 GB of RAM in Preferences > Resources.

#### Demo
```
docker-compose up -d --build
``` 

The above command will start the Jaeger Hotrod sample, Jaeger Agent, OpenTelemetry Collector, Data Prepper, OpenSearch and OpenSearch Dashboards. Wait for few minutes for all the containers to come up, the DataPrepper will restart until OpenSearch becomes available.

After successful start, 

* use the hot rod app at localhost:8080 
* check the traces in OpenSearch Dashboards trace analytics dashboards at localhost:5601/app/trace-analytics-dashboards#/

