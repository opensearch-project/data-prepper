# Elasticsearch Sink Security

This document provides more details about the security settings of the sink.

## AWS Elasticsearch Service

Elasticsearch sink is capable of sending data to Amazon Elasticsearch domain that uses Identity and Access Management. The plugin uses the default credential chain. Run `aws configure` using the AWS CLI to set your credentials. 

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
        "arn:aws:es:us-east-1:<AccountId>:domain/<domain-name>/_alias/otel-v1*"
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

Please check this [doc](https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/es-ac.html) to know how to set IAM to your elasticsearch domain,

### Limitations

* The elasticsearch sink will not work with [custom endpoint](https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/es-customendpoint.html) domains.
* The elasticsearch sink will not with domains that use both IAM and FGAC. (Should be available in next release. :) )

## Opendistro For Elasticsearch

[We are working, deatils soon.]