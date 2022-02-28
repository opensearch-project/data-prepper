#!/usr/bin/env node

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import 'source-map-support/register';
import {App} from 'aws-cdk-lib';
import {GitHubAccessStack} from '../lib/GitHubAccessStack';
import {StagingResourcesStack} from '../lib/StagingResourcesStack';
import {GitHubActionsReleaseAccessStack} from '../lib/GitHubActionsReleaseAccessStack';


const app = new App();

const gitHubAccessStack = new GitHubAccessStack(app, 'GitHubAccessStack', {
  stackName: 'GitHubAccess'
});

const stagingResourcesStack = new StagingResourcesStack(app, 'StagingResourcesStack', {
  stackName: 'StagingResources'
});

new GitHubActionsReleaseAccessStack(app, 'GitHubActionsReleaseAccessStack', {
  stackName: 'GitHubActionsReleaseAccess',
  gitHubOidcProvider: gitHubAccessStack.gitHubOidcProvider,
  ecrRepository: stagingResourcesStack.dataPrepperEcrRepository
});
