/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import * as cdk from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import {StagingResourcesStack} from '../lib/StagingResourcesStack';

test('Creates an ECR Public Repository for Data Prepper', () => {
  const app = new cdk.App();

  const stackUnderTest = new StagingResourcesStack(app, 'TestStack');

  const template = Template.fromStack(stackUnderTest);

  template.hasResourceProperties('AWS::ECR::PublicRepository', {
    RepositoryName: 'data-prepper'
  });
});
