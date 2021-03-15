# Deploying on Amazon EKS
This directory contains an _ingress.yaml_ file capable of deploying an [AWS Application Load Balancer](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/introduction.html)
so that traffic can be routed to the Data Prepper cluster. 

### Prerequisites 
An [AWS Certificate Manager](https://docs.aws.amazon.com/acm/latest/userguide/acm-overview.html) certificate available to be used by the load balancer. Update _ingress.yaml_ with the ARN of the certificate.

Other configuration options can be specified by editing the _ingress.yaml_ file; see the [annotations documentation](https://kubernetes-sigs.github.io/aws-load-balancer-controller/latest/guide/ingress/annotations/) for a complete list of options.

## Steps
1. Provision an Amazon EKS cluster: <https://docs.aws.amazon.com/eks/latest/userguide/create-cluster.html>
2. Configure kubectl to connect with the cluster: <https://aws.amazon.com/premiumsupport/knowledge-center/eks-cluster-connection/>
3. Install the AWS Load Balancer Controller: <https://docs.aws.amazon.com/eks/latest/userguide/aws-load-balancer-controller.html>
4. Run `kubectl apply -f ingress.yaml` in this directory
5. Confirm an ALB with Target Group was created in the Load Balancer AWS Console
