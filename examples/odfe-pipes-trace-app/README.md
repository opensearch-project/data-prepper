# odfe-pipes-trace-app

This repository contains items needed to run our sample application with Docker to generate tracing data. The sample app has a client which makes calls to backend services. The client and the backend services are intrumented OpenTelemetry Trace standards. The traces are submitted to OTel Collector which is then exported to transformation instance. The transformation instace converts the OTEL format data into elasticsearch friendly documents. 

## Architecture

There is one client file:
- client.py

The following are the backend services running on different ports:
- inventoryService.py -> 8082
- databaseService.py -> 8083
- paymentService.py -> 8084
- authenticationService.py -> 8085
- recommendationService.py -> 8086
- order-service -> 8088
- analytics-service -> 8087
- otel-collector -> 55680 
- transformation-instance -> 9400.

The following are the database/storage we run on different ports.
- mysql -> 3306
- elasticsearch -> 9200,9300

The client makes API calls that produces the APM data that falls into the following trace groups:

- load_main_screen
- client_checkout
- client_create_order
- client_cancel_order
- client_delivery_status
- client_pay_order

Correspondingly, on the server side, the API calls are as follows

- /server_request_login (8085) -> /recommend (8086) -> /read_inventory (8082) -> /get_inventory (8083) -> mysql
- /checkout (8084) -> /update_inventory (8082) -> /update_item (8083) -> mysql
- /update_order (8088) -> /add_item_to_cart (8083) -> mysql
- /clear_order (8088) -> /cart_empty (8083) -> mysql
- /get_order (8088) -> /get_cart (8083) -> mysql
- /pay_order (8088) -> /cart_sold (8083) -> mysql



## Run

To run this application together with client:
```
docker-compose up --build -d
```

By default this toy app will write data to the Internet access enabled Amazon elastic search cluster mentioned in the [situp transformation-instance.yml](situp/tranformation-instance.yml).

```
sink:
    amazon_es:
      hosts: ["https://search-sample-app-test-lqwynrnd2ikcuzfsrdilv4stbq.us-west-2.es.amazonaws.com"]
```

If you would like to change the destination to your local elasticsearch update the yaml file with your elastic search address. Example:

Note: The sample uses elasticsearch as part of its architecture, do not use that as your local elastic search.




