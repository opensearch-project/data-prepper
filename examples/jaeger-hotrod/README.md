## Jaeger HotROD

This demo will use the revered [Jaeger HotROD](https://github.com/jaegertracing/jaeger/tree/main/examples/hotrod) app. The purpose of this demo is to experience the Trace Analytics feature using an existing Jaeger based application. For this demo we use the Jaeger HotROD App, Jaeger Agent, OpenTelemetry collector, OpenSearch and Data Prepper.

#### Required:
* Docker - we recommend allowing Docker to use at least 4 GB of RAM in Preferences > Resources.

#### Demo
```
docker compose up -d
``` 

The above command will start the Jaeger HotROD sample, Jaeger Agent, OpenTelemetry Collector, Data Prepper, OpenSearch and OpenSearch Dashboards. Wait for few minutes for all the containers to come up, the DataPrepper container will restart until OpenSearch becomes available.

After the Docker image is running, do the following.

* Open the HotROD app at [http://localhost:8080](http://localhost:8080). Press the buttons in the UI to simulate requests. 
* Log in to the OpenSearch Dashboards Web UI at [http://localhost:5601](http://localhost:5601) using the username `admin` and the password `yourStrongPassword123!`.
* Load the OpenSearch Dashboards trace analytics dashboard at [http://localhost:5601/app/observability-traces#/services](http://localhost:5601/app/observability-traces#/services). If that link does not work, you may still be using On OpenSearch 1.1.0 or below. You will need to use [http://localhost:5601/app/trace-analytics-dashboards#/](http://localhost:5601/app/trace-analytics-dashboards#/) instead. You can view traces and the service map here.

