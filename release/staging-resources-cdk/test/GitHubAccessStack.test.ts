/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import {App} from 'aws-cdk-lib';
import {Template} from 'aws-cdk-lib/assertions';
import {GitHubAccessStack} from '../lib/GitHubAccessStack';

test('Creates OpenId Connect provider for GitHub', () => {
  const app = new App();

  const stackUnderTest = new GitHubAccessStack(app, 'TestStack');

  const template = Template.fromStack(stackUnderTest);

  template.hasResourceProperties('Custom::AWSCDKOpenIdConnectProvider', {
    Url: 'https://token.actions.githubusercontent.com',
    ClientIDList: ['sts.amazonaws.com']
  });
});
