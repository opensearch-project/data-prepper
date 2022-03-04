/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import {Stack, StackProps} from 'aws-cdk-lib';
import {Construct} from 'constructs';
import {
  ManagedPolicy,
  PolicyStatement,
  Effect,
  OpenIdConnectPrincipal,
  Role,
  OpenIdConnectProvider
} from 'aws-cdk-lib/aws-iam'
import {CfnPublicRepository} from 'aws-cdk-lib/aws-ecr';

const DEFAULT_ORGANIZATION = 'opensearch-project'

export interface GitHubActionsReleaseAccessStackProps extends StackProps {
  readonly ecrRepository: CfnPublicRepository;
  readonly gitHubOidcProvider: OpenIdConnectProvider
}

/**
 * Creates the IAM role which GitHub will assume. This role will have all the
 * necessary permissions to deploy artifacts.
 */
export class GitHubActionsReleaseAccessStack extends Stack {

  constructor(scope: Construct, id: string, props: GitHubActionsReleaseAccessStackProps) {
    super(scope, id, props);

    const archivesBucketName: string = scope.node.tryGetContext('archivesBucketName');

    const dataPrepperOrganization: string = scope.node.tryGetContext('dataPrepperOrganization') || DEFAULT_ORGANIZATION;

    const gitHubPrincipal = new OpenIdConnectPrincipal(props.gitHubOidcProvider, {
      'ForAllValues:StringLike': {
        'token.actions.githubusercontent.com:aud': 'sts.amazonaws.com',
        'token.actions.githubusercontent.com:sub': `repo:${dataPrepperOrganization}/data-prepper:ref:refs/heads/*`
      },
    });

    const gitHubActionsReleaseRole = new Role(this, 'GitHubActionsStagingRole', {
      roleName: 'GitHubActionsRelease',
      assumedBy: gitHubPrincipal
    });


    const ecrPublicAuthorizationPolicy = new ManagedPolicy(this, 'ECRPublicAuthorization', {
      managedPolicyName: 'ECRPublicAuthorization',
      statements: [
        new PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            'ecr-public:GetAuthorizationToken',
            'sts:GetServiceBearerToken'
          ],
          resources: ['*'],
        })
      ]
    });

    const ecrPushPolicy = new ManagedPolicy(this, 'ECRPushPolicy', {
      managedPolicyName: 'DataPrepperECRPush',
      statements: [
        new PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            'ecr-public:InitiateLayerUpload',
            'ecr-public:UploadLayerPart',
            'ecr-public:PutImage',
            'ecr-public:CompleteLayerUpload',
            'ecr-public:BatchCheckLayerAvailability'
          ],
          resources: [
            props.ecrRepository.attrArn
          ],
        })
      ]
    });

    const archivesPushPolicy = new ManagedPolicy(this, 'ArchivesPushPolicy', {
      managedPolicyName: 'DataPrepperArchivesPush',
      statements: [
        new PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            's3:PutObject',
            's3:GetObject'
          ],
          resources: [`arn:aws:s3:::${archivesBucketName}/*`],
        }),
        new PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            's3:ListBucket'
          ],
          resources: [`arn:aws:s3:::${archivesBucketName}`],
        }),
        new PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            's3:GetBucketLocation'
          ],
          resources: [`arn:aws:s3:::${archivesBucketName}`],
        })
      ]
    });

    gitHubActionsReleaseRole.addManagedPolicy(ecrPublicAuthorizationPolicy);
    gitHubActionsReleaseRole.addManagedPolicy(ecrPushPolicy);
    gitHubActionsReleaseRole.addManagedPolicy(archivesPushPolicy);
  }
}
