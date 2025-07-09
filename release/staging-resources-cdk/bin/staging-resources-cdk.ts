#!/usr/bin/env node

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import 'source-map-support/register';
import {App} from 'aws-cdk-lib';
import {GitHubAccessStack} from '../lib/GitHubAccessStack';
import {ArchivesBucketStack} from '../lib/ArchivesBucketStack';
import {StagingResourcesStack} from '../lib/StagingResourcesStack';
import {GitHubActionsReleaseAccessStack} from '../lib/GitHubActionsReleaseAccessStack';
import { OpenSearchCIAccessStack } from '../lib/OpenSearchCIAccessStack';


const app = new App();

const gitHubAccessStack = new GitHubAccessStack(app, 'GitHubAccessStack', {
  stackName: 'GitHubAccess'
});

new ArchivesBucketStack(app, 'ArchivesBucketStack', {
  stackName: 'ArchivesBucketStack'
});

const stagingResourcesStack = new StagingResourcesStack(app, 'StagingResourcesStack', {
  stackName: 'StagingResources'
});

new OpenSearchCIAccessStack(app, 'OpenSearchCIAccessStack', {
  stackName: 'DataPrepperStagingResources-OpenSearchCIAccess'
});

new GitHubActionsReleaseAccessStack(app, 'GitHubActionsReleaseAccessStack', {
  stackName: 'GitHubActionsReleaseAccess',
  gitHubOidcProvider: gitHubAccessStack.gitHubOidcProvider,
  ecrRepository: stagingResourcesStack.dataPrepperEcrRepository
});
