## Jaeger Hot Rod

This demo will use the Jaeger Hotrod app. 

#### Required:
* Docker - If you run Docker locally, we recommend allowing Docker to use at least 4 GB of RAM in Preferences > Resources.

#### Demo
```
docker-compose up -d --build
``` 

The above command will start the Jaeger Hotrod sample, Jaeger Agent, OpenTelemetry Collector, Data Prepper, ODFE and Kibana.

After successful start, 

* use the hot rod app at localhost:8080 
* check the traces in kibana trace analytics dashboards at localhost:5601/app/traceAnalytics#/dashboard

