# Data Prepper Staging Resources CDK

This project provides a CDK stack for deploying Data Prepper resources to a staging environment.
This creates an AWS environment suitable for hosting Data Prepper artifacts.

*You probably don't need to use this sub-project. It is intended for releasing Data Prepper.
If you do want to distribute your own Data Prepper releases, read on.*


## Build & Test

### Build Prerequisites

You must install CDK and Node.js first.


### Build

You must run all the following commands from the `release/staging/resources-cdk` directory. 

The first time you use the CDK, run:

```
npm install
```

### Testing

You can run the following command to run the CDK tests.

```
npm run test
```

If you have made code changes, please run the linter to verify that your code is correctly formatted.

```
npm run lint
```

## Deployment

### AWS Prerequisites

You must have an AWS account and administrator permissions to this account.

This stack supports creating an S3 bucket or using an existing S3 bucket.

### Deploy Steps

The following CDK commands all require defining context. The context variables are:

* `archivesBucketName` - The name of the S3 bucket you will use to deploy. This bucket must be in the same region as your stack.
* `dataPrepperOrganization` - The name of the GitHub organization which has the `data-prepper` repository. This allows you to create staging environments for forks. The default value is `opensearch-project`.
* `ciAccountId` - The AWS account Id of the OpenSearch CI release/build server.

The following command will deploy the CDK stack and create a new S3 bucket. If you'd like to use an existing S3 bucket, see the section below
for deploying individual stacks.

```
cdk deploy --all --context archivesBucketName={s3-bucket-name} --context dataPrepperOrganization={data-prepper-organization-name} --context ciAccountId={opensearch-ci-account-id}
```

#### Deploy to Use an Existing S3 Bucket

To use an existing bucket, you must deploy the stacks independently.

Deploy the GitHub access stack:

```
cdk deploy GitHubAccessStack --context archivesBucketName={s3-bucket-name} --context dataPrepperOrganization={data-prepper-organization-name}
```

Deploy the stack with the actual staging resources:

```
cdk deploy StagingResourcesStack --context archivesBucketName={s3-bucket-name} --context dataPrepperOrganization={data-prepper-organization-name}
```

Deploy the stack which gives GitHub Actions permissions to deploy to the staging resources.

```
cdk deploy GitHubActionsReleaseAccessStack --context archivesBucketName={s3-bucket-name} --context dataPrepperOrganization={data-prepper-organization-name}
```
