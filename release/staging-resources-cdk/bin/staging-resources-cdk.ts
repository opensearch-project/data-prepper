#!/usr/bin/env node

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import 'source-map-support/register';
import {App} from 'aws-cdk-lib';
import {GitHubAccessStack} from '../lib/GitHubAccessStack';
import {StagingResourcesStack} from '../lib/StagingResourcesStack';


const app = new App();

new GitHubAccessStack(app, 'GitHubAccessStack', {
  stackName: 'GitHubAccess'
});

new StagingResourcesStack(app, 'StagingResourcesStack', {
  stackName: 'StagingResources'
});
