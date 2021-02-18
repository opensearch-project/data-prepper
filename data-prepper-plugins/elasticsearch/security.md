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

### Limitations

* The Elasticsearch sink will not work with [custom endpoint](https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/es-customendpoint.html) domains.
* The Elasticsearch sink will not with domains that use both IAM and FGAC. (Should be available in next release. :) )

## Opendistro For Elasticsearch

Elasticsearch sink can send data to opendistro-for-elasticsearch (ODFE) cluster by administrative credentials as follows:

```
sink:
  - elasticsearch:
      ...
      username: "admin"
      password: "admin"
```

or through internal user credential assigned with roles of required permissions. With administrative privilege, one can create an internal user, a role 
and map the user to the role by following the ODFE [instructions](https://opendistro.github.io/for-elasticsearch-docs/docs/security/access-control/users-roles/). 
For sending data to ODFE, one need the following minimum permissions assigned to the role:

### Cluster permissions

- `cluster_all`
- `indices:admin/template/get`
- `indices:admin/template/put`

Note that `indices:admin/template/*` need to be in cluster permissions.

### Index permissions

- `Index`: `otel-v1*`; `Index permissions`: `indices_all`
- `Index`: `.opendistro-ism-config`; `Index permissions`: `indices_all`

`Field level security` and `Anonymization` should be left with default values.