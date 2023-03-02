/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import {Stack, StackProps} from 'aws-cdk-lib';
import {Construct} from 'constructs';
import {OpenIdConnectProvider} from 'aws-cdk-lib/aws-iam'

/**
 * Creates the IAM resources necessary for GitHub to access roles within
 * this account.
 */
export class GitHubAccessStack extends Stack {
  readonly gitHubOidcProvider: OpenIdConnectProvider;

  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    this.gitHubOidcProvider = new OpenIdConnectProvider(this, 'GitHubOidcProvider', {
      url: 'https://token.actions.githubusercontent.com',
      thumbprints: [
        '6938fd4d98bab03faadb97b34396831e3780aea1'
      ],
      clientIds: ['sts.amazonaws.com']
    });
  }
}
