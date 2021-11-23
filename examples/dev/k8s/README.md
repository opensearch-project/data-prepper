# Example Kubernetes Deployment with Minikube

This set of Kubernetes config files recreates the same sample app under the /examples directory, but within a minikube environment.
* _data-prepper.yaml_ contains a deployment, service, headless service, and configmap required to run Data Prepper with Kubernetes
* _sample-project-applications.yaml_ contains configuration required to setup the remaining elements of the sample project (OpenSearch, OpenSearch Dashboards, etc.)

1. Setup kubectl and minikube locally
   1. https://kubernetes.io/docs/tasks/tools/install-kubectl/
   2. https://minikube.sigs.k8s.io/docs/start/
2. `minikube start`
3. `./build_images_for_minikube.sh` to build required Docker images locally 
4. `kubectl apply -f .`
5. To cleanup, `kubectl delete -f .`

Once the k8s deployment is running, you can check the status via `minikube dashboard` and visiting the output URL in a browser.
