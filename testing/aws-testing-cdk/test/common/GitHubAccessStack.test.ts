/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import {App} from 'aws-cdk-lib';
import {Template} from 'aws-cdk-lib/assertions';
import {GitHubAccessStack} from '../../lib/common/GitHubAccessStack';

test('Creates OpenId Connect provider for GitHub', () => {
  const app = new App();

  const stackUnderTest = new GitHubAccessStack(app, 'TestStack');

  const template = Template.fromStack(stackUnderTest);

  template.hasResourceProperties('Custom::AWSCDKOpenIdConnectProvider', {
    Url: 'https://token.actions.githubusercontent.com',
    ClientIDList: ['sts.amazonaws.com']
  });

  template.hasResourceProperties('AWS::IAM::Role', {
    RoleName: 'GitHubActionsTesting',
  });
});

test('Uses existing OpenId Connect provider for GitHub when gitHubOidcProviderExists', () => {
  const app = new App();
  app.node.setContext('gitHubOidcProviderExists', true)

  const stackUnderTest = new GitHubAccessStack(app, 'TestStack');

  const template = Template.fromStack(stackUnderTest);

  template.resourceCountIs('Custom::AWSCDKOpenIdConnectProvider', 0)

  template.hasResourceProperties('AWS::IAM::Role', {
    RoleName: 'GitHubActionsTesting',
  });
});
