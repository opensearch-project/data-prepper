# Elasticsearch Sink Security

This document provides more details about the security settings of the sink.

## AWS Elasticsearch Service

Elasticsearch sink is capable of sending data to Amazon Elasticsearch domain which use Identity and Access Management. The plugin uses the default credential chain. Run `aws configure` using the AWS CLI to set your credentials. 

You should ensure that the credentials you configure have the required permissions. Below is an example Resource based policy, with required set of permissions that is required for the sink to work,

```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::<AccountId>:user/data-prepper-sink-user"
      },
      "Action": "es:ESHttp*",
      "Resource": [
        "arn:aws:es:us-east-1:<AccountId>:domain/<domain-name>/otel-v1*",
        "arn:aws:es:us-east-1:<AccountId>:domain/<domain-name>/_template/otel-v1*",
        "arn:aws:es:us-east-1:<AccountId>:domain/<domain-name>/_opendistro/_ism/policies/raw-span-policy",
        "arn:aws:es:us-east-1:<AccountId>:domain/<domain-name>/_alias/otel-v1*",
        "arn:aws:es:us-east-1:<AccountId>:domain/<domain-name>/_alias/_bulk"
      ]
    },
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::<AccountId>:user/data-prepper-sink-user"
      },
      "Action": "es:ESHttpGet",
      "Resource": "arn:aws:es:us-east-1:<AccountId>:domain/<domain-name>/_cluster/settings"
    }
  ]
}
``` 

Please check this [doc](https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/es-ac.html) to know how to set IAM to your Elasticsearch domain,

### Fine-Grained Access Control in Amazon Elasticsearch Service

The Elasticsearch sink creates ISM policy for Trace Analytics indices and Amazon Elasticsearch service allows only the `master user` to create ISM policy. So if you use,
 
 * IAM for your master user in FGAC domain, configure the sink as below,
  
  ```
  sink:
      elasticsearch:
        hosts: ["https://your-fgac-amazon-elasticssearch-service-endpoint"]
        aws_sigv4: true 
  ```
 
 * Internal database for your master user in FGAC domain, configure the sink as below,
 
 ```
 sink:
     elasticsearch:
       hosts: ["https://your-fgac-amazon-elasticssearch-service-endpoint"]
       aws_sigv4: false
       username: "master-username"
       password: "master-password" 
 ```

### Limitations

* The Elasticsearch sink will not work with [custom endpoint](https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/es-customendpoint.html) domains.