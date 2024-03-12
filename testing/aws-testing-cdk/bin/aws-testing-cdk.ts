#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import {GitHubAccessStack} from '../lib/common/GitHubAccessStack';
import {SecretsManagerStack} from '../lib/aws-secrets-manager/SecretsManagerStack';
import {KmsStack} from '../lib/common/KmsStack';

const app = new cdk.App();

const githubStack = new GitHubAccessStack(app, 'GitHubStack', {});

new KmsStack(app, 'CommonKmsStack', {
  testingRole: githubStack.gitHubActionsTestingRole
})

new SecretsManagerStack(app, 'SecretsManagerStack', {
  testingRole: githubStack.gitHubActionsTestingRole
});
