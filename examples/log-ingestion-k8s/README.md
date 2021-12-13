# Kubernetes Logging with Fluent Bit and Data-Prepper

[Fluent Bit](http://fluentbit.io/) is a lightweight and extensible __Log and Metrics Processor__ that comes with full support for Kubernetes:

* Read Kubernetes/Docker log files from the file system or through systemd Journal
* Enrich logs with Kubernetes metadata
* Deliver logs to third party services like Elasticsearch, Splunk, Datadog, InfluxDB, HTTP, etc.
This repository contains a set of Yaml files to deploy Fluent Bit which consider namespace, RBAC, Service Account, etc.

This example uses [minikube](https://minikube.sigs.k8s.io/docs/) to demo Fluent Bit to Data-Prepper pipeline that collects and ingest logs from `my-app` [namespace](https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/). 
The service architecture is illustrated as follows:
![Architecture](K8-fluentbit-data-prepper.png)
We deploy two `nginx` [pods](https://kubernetes.io/docs/concepts/workloads/pods/) and expose a `my-nginx` [service](https://kubernetes.io/docs/concepts/services-networking/service/) on top of them in the `my-app` namespace. Then in the `logging` namespace,
we deploy Fluent Bit as [DaemonSet](https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/) that runs in every node in Kubernetes collects and forwards logs to an pre-existing Data-Prepper endpoint, which is located outside 
Kubernetes cluster.

## Getting started

### Prerequisite
A Data-Prepper endpoint with [example_log_pipeline.yaml](data-prepper-pipeline-config/example_log_pipeline.yaml) configured.

### Steps
1. Install [Docker](https://docs.docker.com/get-docker/).
2. Setup kubectl and minikube locally
    1. https://kubernetes.io/docs/tasks/tools/install-kubectl/
    2. https://minikube.sigs.k8s.io/docs/start/
Notice that this demo uses minikube > 1.21.
3. Replace the `Host` value in `output-data-prepper.conf` in [fluent-bit-05-configmap.yaml](fluent-bit-05-configmap.yaml) with your Data-Prepper endpoint. For example,
if the data-prepper is running locally, set `Host` value to be `host.docker.internal`.
4. `minikube start`
5. You could run `kubectl apply -f .` to deploy everything or run step-by-step as follows:
   1. Deploy two `nginx` pods as sample application in `my-app` namespace and expose them as `my-nginx` service:
   ```
   $ kubectl apply -f my-app-01-ns.yaml
   $ kubectl create -f my-app-02-service.yaml
   $ kubectl create -f my-app-03-deployment.yaml
   ```
   2. Deploy FluentBit as DaemonSet in `logging` namespace:
   ```
   $ kubectl apply -f fluent-bit-01-ns.yaml
   $ kubectl create -f fluent-bit-02-service-account.yaml
   $ kubectl create -f fluent-bit-03-role-1.22.yaml
   $ kubectl create -f fluent-bit-04-role-binding-1.22.yaml
   $ kubectl create -f fluent-bit-05-configmap.yaml
   $ kubectl create -f fluent-bit-06-ds-minikube.yaml
   ```
6. Tunneling `my-nginx` service through minikube:
   ```
   $ minikube service my-nginx -n my-app
   ```
   example output:
   ```
   😿  service default/my-nginx has no node port
   🏃  Starting tunnel for service my-nginx.
   |-----------|----------|-------------|------------------------|
   | NAMESPACE |   NAME   | TARGET PORT |          URL           |
   |-----------|----------|-------------|------------------------|
   | default   | my-nginx |             | http://127.0.0.1:62907 |
   |-----------|----------|-------------|------------------------|
   ```
7. You could now curl the URL by
   ```
   curl http://127.0.0.1:62907
   ```
   and see response as follows:
   ```
   <!DOCTYPE html>
   <html>
   <head>
   <title>Welcome to nginx!</title>
   <style>
   html { color-scheme: light dark; }
   body { width: 35em; margin: 0 auto;
   font-family: Tahoma, Verdana, Arial, sans-serif; }
   </style>
   </head>
   <body>
   <h1>Welcome to nginx!</h1>
   <p>If you see this page, the nginx web server is successfully installed and
   working. Further configuration is required.</p>
   
   <p>For online documentation and support please refer to
   <a href="http://nginx.org/">nginx.org</a>.<br/>
   Commercial support is available at
   <a href="http://nginx.com/">nginx.com</a>.</p>
   
   <p><em>Thank you for using nginx.</em></p>
   </body>
   </html>
   ```
The deployed `nginx` pods will generate apache logs accordingly and Fluent Bit DaemonSet will collect, parse, filter and output logs to Data-Prepper backend. 
8. By configuring `stdout` as sink, the Data-Prepper instance logs should now print out sample records as follows:
```
{"date":1.639425394678687E9,"log":"172.17.0.1 - - [13/Dec/2021:19:56:34 +0000] \"GET / HTTP/1.1\" 200 615 \"-\" \"curl/7.64.1\" \"-\"\n","stream":"stdout","time":"2021-12-13T19:56:34.6786871Z","kubernetes":{"pod_name":"my-nginx-5b56ccd65f-zfvtj","namespace_name":"my-app","pod_id":"2989f623-5a65-4caf-b33a-95154a04f53b","labels":{"pod-template-hash":"5b56ccd65f","run":"my-nginx"},"host":"minikube","container_name":"my-nginx","docker_id":"f827ae8f8ff66175da32776b3875fe10916378e89645288d415edfd22f060fdb","container_hash":"nginx@sha256:9522864dd661dcadfd9958f9e0de192a1fdda2c162a35668ab6ac42b465f0603","container_image":"nginx:latest"},"request":"/","auth":"-","ident":"-","response":"200","bytes":"615","clientip":"172.17.0.1","verb":"GET","httpversion":"1.1","timestamp":"13/Dec/2021:19:56:34 +0000"}
```
8. To clean up the resources created in minikube
   ```
   kubectl delete -f .
   ```
9. Stop minikube
   ```
   minikube stop
   ```