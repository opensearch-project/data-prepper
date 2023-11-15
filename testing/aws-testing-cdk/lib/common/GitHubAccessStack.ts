/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import {Stack, StackProps} from 'aws-cdk-lib';
import {Construct} from 'constructs';
import {OpenIdConnectPrincipal, OpenIdConnectProvider, Role} from 'aws-cdk-lib/aws-iam'

const DEFAULT_ORGANIZATION = 'opensearch-project'

/**
 * Creates the IAM resources necessary for GitHub to access roles within
 * this account.
 */
export class GitHubAccessStack extends Stack {
  readonly gitHubActionsTestingRole: Role;

  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const gitHubOidcProvider = new OpenIdConnectProvider(this, 'GitHubOidcProvider', {
      url: 'https://token.actions.githubusercontent.com',
      thumbprints: [
        '6938fd4d98bab03faadb97b34396831e3780aea1'
      ],
      clientIds: ['sts.amazonaws.com']
    });

    const dataPrepperOrganization: string = scope.node.tryGetContext('dataPrepperOrganization') || DEFAULT_ORGANIZATION;

    const gitHubPrincipal = new OpenIdConnectPrincipal(gitHubOidcProvider, {
      'ForAllValues:StringLike': {
        'token.actions.githubusercontent.com:aud': 'sts.amazonaws.com',
        'token.actions.githubusercontent.com:sub': `repo:${dataPrepperOrganization}/data-prepper:ref:refs/heads/*`
      },
    });

    this.gitHubActionsTestingRole = new Role(this, 'GitHubActionsTestingRole', {
      roleName: 'GitHubActionsTesting',
      assumedBy: gitHubPrincipal
    });
  }
}
