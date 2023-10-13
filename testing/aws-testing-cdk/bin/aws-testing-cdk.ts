#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import {GitHubStack} from '../lib/common/GitHubStack';
import {SecretsManagerStack} from '../lib/aws-secrets-manager/SecretsManagerStack';

const app = new cdk.App();

const githubStack = new GitHubStack(app, 'GitHubStack', {});

new SecretsManagerStack(app, 'SecretsManagerStack', {
  testingRole: githubStack.gitHubActionsTestingRole
});