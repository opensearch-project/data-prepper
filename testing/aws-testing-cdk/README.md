# Data Prepper AWS Testing Resources CDK

This project provides a CDK stack for deploying Data Prepper AWS testing resources to a staging environment.

The `cdk.json` file tells the CDK Toolkit how to execute your app.

## Build & Test

### Build Prerequisites

You must install CDK and Node.js first.


### Build

You must run all the following commands from the `testing/aws-testing-cdk` directory.

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

### Deploy Steps

The following CDK commands all require defining context. The context variables are:

* `dataPrepperOrganization` - The name of the GitHub organization which has the `data-prepper` repository. This allows you to create staging environments for forks. The default value is `opensearch-project`.

The following command will deploy all the CDK stack.

```
cdk deploy --all --context dataPrepperOrganization={data-prepper-organization-name}
```

## Useful commands

* `npm run build`   compile typescript to js
* `npm run watch`   watch for changes and compile
* `npm run test`    perform the jest unit tests
* `cdk deploy`      deploy this stack to your default AWS account/region
* `cdk diff`        compare deployed stack with current state
* `cdk synth`       emits the synthesized CloudFormation template
