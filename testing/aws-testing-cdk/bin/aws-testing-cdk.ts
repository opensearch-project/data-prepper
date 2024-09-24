#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import {GitHubAccessStack} from '../lib/common/GitHubAccessStack';
import {SecretsManagerStack} from '../lib/aws-secrets-manager/SecretsManagerStack';
import {KmsStack} from '../lib/common/KmsStack';
import {S3SinkStack} from '../lib/s3/S3SinkStack';
import { KinesisSourceStack } from '../lib/kinesis/KinesisSourceStack';

const app = new cdk.App();

const githubStack = new GitHubAccessStack(app, 'GitHubStack', {});

new KmsStack(app, 'CommonKmsStack', {
  testingRole: githubStack.gitHubActionsTestingRole
})

new SecretsManagerStack(app, 'SecretsManagerStack', {
  testingRole: githubStack.gitHubActionsTestingRole
});

new S3SinkStack(app, 'S3SinkStack', {
  testingRole: githubStack.gitHubActionsTestingRole
});

new KinesisSourceStack(app, 'KinesisSourceStack', {
  testingRole: githubStack.gitHubActionsTestingRole
});