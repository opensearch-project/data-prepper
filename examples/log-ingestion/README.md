# Data Prepper Log Ingestion Demo Guide

This is a guide that will walk users through setting up a sample Data Prepper pipeline for log ingestion. 
This guide will go through the steps required to create a simple log ingestion pipeline from \
Fluent Bit → Data Prepper → OpenSearch. This log ingestion flow is shown in the diagram below.

![](../../docs/images/Log_Ingestion_FluentBit_DataPrepper_OpenSearch.jpg)

## List of Components

- An OpenSearch domain running through Docker
- A FluentBit agent running through Docker
- Data Prepper, which includes a `log_pipeline.yaml`
- An Apache Log Generator in the form of a python script

### FluentBit And OpenSearch Setup

1. Take a look at the [docker-compose.yaml](docker-compose.yaml). This `docker-compose.yaml` will pull the FluentBit and OpenSearch Docker images and run them in the `log-ingestion_opensearch-net` Docker network.


2. Now take a look at the [fluent-bit.conf](fluent-bit.conf). This config will tell FluentBit to tail the `/var/log/test.log` file for logs, and uses the FluentBit http output plugin to forward these logs to the http source of Data Prepper, which runs by default on port 2021. The `fluent-bit.conf` file
is mounted as a Docker volume through the `docker-compose.yaml`.


3. An empty file named `test.log` has been created. This file is also mounted through the  `docker-compose.yaml`, and will be the file
FluentBit is tailing to collect logs from.
   

4. Now that you understand a bit more about how FluentBit and OpenSearch are set up, run them with:

```
docker-compose --project-name data-prepper up
```

### Data Prepper Setup

1. Pull down the latest Data Prepper Docker image.

```
docker pull opensearchproject/data-prepper:2
```
 
2. Take a look at [log_pipeline.yaml](log_pipeline.yaml). This configuration will take logs sent to the [http source](../../data-prepper-plugins/http-source), 
process them with the [Grok Processor](../../data-prepper-plugins/grok-prepper) by matching against the `COMMONAPACHELOG` pattern, 
and send the processed logs to a local [OpenSearch sink](../../data-prepper-plugins/opensearch) to an index named `apache_logs`.


3. Run the Data Prepper docker image with the `log_pipeline.yaml` from step 2 passed in. This command attaches the Data Prepper Docker image to the Docker network `log-ingestion_opensearch_net` so that 
FluentBit is able to send logs to the http source of Data Prepper.

Run the following to start Data Prepper:

```
docker run --name data-prepper -v ${PWD}/log_pipeline.yaml:/usr/share/data-prepper/pipelines/log_pipeline.yaml --network "data-prepper_opensearch-net" opensearchproject/data-prepper:2
```

If Data Prepper is running correctly, you should see something similar to the following line as the latest output in your terminal.

```
INFO  org.opensearch.dataprepper.pipeline.ProcessWorker - log-pipeline Worker: No records received from buffer
```

### Apache Log Generator

Note that if you just want to see the log ingestion workflow in action, you can simply copy and paste some logs into the `test.log` file yourself without using the Python [Fake Apache Log Generator](https://github.com/graytaylor0/Fake-Apache-Log-Generator). 
Here is a sample batch of randomly generated Apache Logs if you choose to take this route.

```
63.173.168.120 - - [04/Nov/2021:15:07:25 -0500] "GET /search/tag/list HTTP/1.0" 200 5003
71.52.186.114 - - [04/Nov/2021:15:07:27 -0500] "GET /search/tag/list HTTP/1.0" 200 5015
223.195.133.151 - - [04/Nov/2021:15:07:29 -0500] "GET /posts/posts/explore HTTP/1.0" 200 5049
249.189.38.1 - - [04/Nov/2021:15:07:31 -0500] "GET /app/main/posts HTTP/1.0" 200 5005
36.155.45.2 - - [04/Nov/2021:15:07:33 -0500] "GET /search/tag/list HTTP/1.0" 200 5001
4.54.90.166 - - [04/Nov/2021:15:07:35 -0500] "DELETE /wp-content HTTP/1.0" 200 4965
214.246.93.195 - - [04/Nov/2021:15:07:37 -0500] "GET /apps/cart.jsp?appID=4401 HTTP/1.0" 200 5008
72.108.181.108 - - [04/Nov/2021:15:07:39 -0500] "GET /wp-content HTTP/1.0" 200 5020
194.43.128.202 - - [04/Nov/2021:15:07:41 -0500] "GET /app/main/posts HTTP/1.0" 404 4943
14.169.135.206 - - [04/Nov/2021:15:07:43 -0500] "DELETE /wp-content HTTP/1.0" 200 4985
208.0.179.237 - - [04/Nov/2021:15:07:45 -0500] "GET /explore HTTP/1.0" 200 4953
134.29.61.53 - - [04/Nov/2021:15:07:47 -0500] "GET /explore HTTP/1.0" 200 4937
213.229.161.38 - - [04/Nov/2021:15:07:49 -0500] "PUT /posts/posts/explore HTTP/1.0" 200 5092
82.41.77.121 - - [04/Nov/2021:15:07:51 -0500] "GET /app/main/posts HTTP/1.0" 200 5016
```

Additionally, if you just want to test a single log, you can send it to `test.log` directly with:

```
echo '63.173.168.120 - - [04/Nov/2021:15:07:25 -0500] "GET /search/tag/list HTTP/1.0" 200 5003' >> test.log
```

In order to simulate an application generating logs, a simple python script will be used. This script only runs with python 2. You can download this script by running

```
git clone https://github.com/graytaylor0/Fake-Apache-Log-Generator.git
```

Note the requirements in the README of the Apache Log Generator. You must have Python 2.7 and you must run 
```
pip install -r requirements.txt
```

to install the necessary dependencies.

Run the apache log generator python script so that it sends an apache log to the `test.log` file from the fluent-bit `docker-compose.yaml` every 2 seconds. 

```
python apache-fake-log-gen.py -n 0 -s 2 -l "CLF" -o "LOG" -f "/full/path/to/test.log"
```

You should now be able to check your terminal output for FluentBit and Data Prepper to verify that they are processing logs.

The following FluentBit ouptut means that FluentBit was able to forward logs to the Data Prepper http source

```
fluent-bit  | [2021/10/30 17:16:39] [ info] [output:http:http.0] host.docker.internal:2021, HTTP status=200
```

The following Data Prepper output indicates that Data Prepper is successfully processing logs from FluentBit

```
2021-10-30T12:17:17,474 [log-pipeline-prepper-worker-1-thread-1] INFO  org.opensearch.dataprepper.pipeline.ProcessWorker -  log-pipeline Worker: Processing 2 records from buffer
```

Finally, head into OpenSearch Dashboards ([http://localhost:5601](http://localhost:5601)) to view your processed logs.
You will need to create an index pattern for the index provided in your `pipeline.yaml` in order to see them. You can do this by going to
`Stack Management -> Index Pattterns`. Now start typing in the name of the index you sent logs to (in this guide it was `apache_logs`),
and you should see that the index pattern matches 1 source. Click `Create Index Pattern`, and you should then be able to go back to 
the `Discover` tab to see your processed logs. 
