# Deploying with CloudFormation
This directory contains an [AWS CloudFormation](https://docs.aws.amazon.com/cloudformation/index.html) template which provisions a single Data Prepper instance on an EC2 host. The template's parameters allow you to configure settings such as the EC2 instance type, Amazon OpenSearch Service endpoint, and which SSH key pair can be used to access the host. 

Data Prepper server and pipeline configuration values can be manually adjusted by editing the _Resources_ section of the yaml template.
