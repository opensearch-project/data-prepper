## Zipkin sleuth-webmvc-example

This demo will use the [sleuth-webmvc-example](https://github.com/openzipkin/sleuth-webmvc-example). 

#### Required:
* Docker - If you run Docker locally, we recommend allowing Docker to use at least 4 GB of RAM in Preferences > Resources.

#### Demo
```
docker-compose up -d --build
``` 

The above command will start the frontend, backend, Zipkin, OpenTelemetry Collector, Data Prepper, ODFE and Kibana.

After successful start, one can send request to the frontend by opening http://localhost:8081/ or running curl command, e.g. 

```$xslt
curl -s localhost:8081 -H'user_name: JC'
```

Now check the traces in kibana which runs at localhost:5601.

You will see two indicies,

* otel-v1-apm-span  - This index alias will store the raw traces.
* otel-v1-apm-service-map - This index will store the relationship between services.