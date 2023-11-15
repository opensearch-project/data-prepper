#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import {GitHubAccessStack} from '../lib/common/GitHubAccessStack';
import {SecretsManagerStack} from '../lib/aws-secrets-manager/SecretsManagerStack';

const app = new cdk.App();

const githubStack = new GitHubAccessStack(app, 'GitHubStack', {});

new SecretsManagerStack(app, 'SecretsManagerStack', {
  testingRole: githubStack.gitHubActionsTestingRole
});