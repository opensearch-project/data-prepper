/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

import {Stack, StackProps} from 'aws-cdk-lib';
import {Construct} from 'constructs';
import {
  AccountPrincipal,
  CompositePrincipal,
  OpenIdConnectPrincipal,
  OpenIdConnectProvider,
  PolicyStatement,
  Role
} from 'aws-cdk-lib/aws-iam'

const DEFAULT_ORGANIZATION = 'opensearch-project'
const GITHUB_TOKEN_URL = 'token.actions.githubusercontent.com'

/**
 * Creates the IAM resources necessary for GitHub to access roles within
 * this account.
 */
export class GitHubAccessStack extends Stack {
  readonly gitHubActionsTestingRole: Role;

  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const oidcProviderExists: boolean = scope.node.tryGetContext('gitHubOidcProviderExists');

    const gitHubOidcProvider =
      oidcProviderExists ?
        OpenIdConnectProvider.fromOpenIdConnectProviderArn(this, 'GitHubOidcProvider', `arn:aws:iam::${this.account}:oidc-provider/${GITHUB_TOKEN_URL}`) :
        new OpenIdConnectProvider(this, 'GitHubOidcProvider', {
          url: `https://${GITHUB_TOKEN_URL}`,
          thumbprints: [
            '6938fd4d98bab03faadb97b34396831e3780aea1'
          ],
          clientIds: ['sts.amazonaws.com']
        });

    const dataPrepperOrganization: string = scope.node.tryGetContext('dataPrepperOrganization') || DEFAULT_ORGANIZATION;

    const gitHubPrincipal = new OpenIdConnectPrincipal(gitHubOidcProvider, {
      'ForAllValues:StringLike': {
        'token.actions.githubusercontent.com:aud': 'sts.amazonaws.com',
        'token.actions.githubusercontent.com:sub': `repo:${dataPrepperOrganization}/data-prepper:*`
      },
    });

    const currentAccountPrincipal = new AccountPrincipal(this.account);

    this.gitHubActionsTestingRole = new Role(this, 'GitHubActionsTestingRole', {
      roleName: 'GitHubActionsTesting',
      assumedBy: new CompositePrincipal(
        gitHubPrincipal,
        currentAccountPrincipal
      )
    });

    this.gitHubActionsTestingRole.addToPolicy(new PolicyStatement({
      actions: ['cloudformation:DescribeStacks'],
      resources: [`arn:aws:cloudformation:*:${this.account}:stack/*`],
    }));
  }
}
