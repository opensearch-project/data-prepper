/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import {App, Stack} from 'aws-cdk-lib';
import {Match, Template} from 'aws-cdk-lib/assertions';
import {GitHubActionsReleaseAccessStack} from '../lib/GitHubActionsReleaseAccessStack';
import {OpenIdConnectProvider} from 'aws-cdk-lib/aws-iam';
import {CfnPublicRepository} from 'aws-cdk-lib/aws-ecr';


let app: App;

beforeEach(() => {
  app = new App({
    context: {
      archivesBucketName: 'test-s3-bucket-name'
    }
  });
});

function createOidcProvider(): OpenIdConnectProvider {
  const stack = new Stack(app, 'OidcStack');
  return new OpenIdConnectProvider(stack, 'TestOidcProvider', {
    url: 'https://aws.amazon.com/example'
  });
}

function createEcrRepository(): CfnPublicRepository {
  const stack = new Stack(app, 'EcrRepositoryStack');
  return new CfnPublicRepository(stack, 'TestECRRepository', {
    repositoryName: 'sample-repository'
  });
}

function createStackUnderTest() {
  return new GitHubActionsReleaseAccessStack(app, 'StackUnderTest', {
    gitHubOidcProvider: createOidcProvider(),
    ecrRepository: createEcrRepository()
  });
}

test('Creates Role with the correct assume role if no organization is supplied', () => {
  const stackUnderTest = createStackUnderTest();

  const template = Template.fromStack(stackUnderTest);

  template.hasResourceProperties('AWS::IAM::Role', {
    RoleName: 'GitHubActionsRelease',
    AssumeRolePolicyDocument: {
      Statement: [
        {
          Action: 'sts:AssumeRoleWithWebIdentity',
          Condition: {
            'ForAllValues:StringLike': {
              'token.actions.githubusercontent.com:aud': 'sts.amazonaws.com',
              'token.actions.githubusercontent.com:sub': 'repo:opensearch-project/data-prepper:ref:refs/heads/*'
            }
          },
          Effect: 'Allow',
          Principal: {
            Federated: {
              'Fn::ImportValue': Match.anyValue()
            }
          }
        }
      ]
    }
  });
});

test('Creates Role with the correct assume role when the organization is provided', () => {

  app = new App({
    context: {
      'dataPrepperOrganization': 'test-organization'
    }
  });

  const stackUnderTest = createStackUnderTest();

  const template = Template.fromStack(stackUnderTest);

  template.hasResourceProperties('AWS::IAM::Role', {
    RoleName: 'GitHubActionsRelease',
    AssumeRolePolicyDocument: {
      Statement: [
        {
          Action: 'sts:AssumeRoleWithWebIdentity',
          Condition: {
            'ForAllValues:StringLike': {
              'token.actions.githubusercontent.com:aud': 'sts.amazonaws.com',
              'token.actions.githubusercontent.com:sub': 'repo:test-organization/data-prepper:ref:refs/heads/*'
            }
          },
          Effect: 'Allow',
          Principal: {
            Federated: {
              'Fn::ImportValue': Match.anyValue()
            }
          }
        }
      ]
    }
  });
});

test('Creates Managed Policy with permissions to authenticate with ECR Public', () => {

  const stackUnderTest = createStackUnderTest();

  const template = Template.fromStack(stackUnderTest);

  template.hasResourceProperties('AWS::IAM::ManagedPolicy', {
    ManagedPolicyName: 'ECRPublicAuthorization',
    PolicyDocument: {
      Statement: [
        {
          Action: [
            'ecr-public:GetAuthorizationToken',
            'sts:GetServiceBearerToken'
          ],
          Effect: 'Allow',
          Resource: '*'
        }
      ]
    }
  });
});

test('Creates Managed Policy with permissions to push to the S3 bucket', () => {

  const stackUnderTest = createStackUnderTest();

  const template = Template.fromStack(stackUnderTest);

  template.hasResourceProperties('AWS::IAM::ManagedPolicy', {
    ManagedPolicyName: 'DataPrepperArchivesPush',
    PolicyDocument: {
      Statement: [
        {
          Action: [
            's3:PutObject',
            's3:GetObject'
          ],
          Effect: 'Allow',
          Resource: 'arn:aws:s3:::test-s3-bucket-name/*'
        },
        {
          Action: 's3:ListBucket',
          Effect: 'Allow',
          Resource: 'arn:aws:s3:::test-s3-bucket-name'
        },
        {
          Action: 's3:GetBucketLocation',
          Effect: 'Allow',
          Resource: 'arn:aws:s3:::test-s3-bucket-name'
        }
      ]
    }
  });
});

test('Creates Managed Policy with permissions to push to the ECR repository', () => {

  const gitHubOidcProvider = createOidcProvider();

  const stackUnderTest = new GitHubActionsReleaseAccessStack(app, 'StackUnderTest', {
    gitHubOidcProvider: gitHubOidcProvider,
    ecrRepository: createEcrRepository()
  });

  const template = Template.fromStack(stackUnderTest);

  template.hasResourceProperties('AWS::IAM::ManagedPolicy', {
    ManagedPolicyName: 'DataPrepperECRPush',
    PolicyDocument: {
      Statement: [
        {
          Action: [
            'ecr-public:InitiateLayerUpload',
            'ecr-public:UploadLayerPart',
            'ecr-public:PutImage',
            'ecr-public:CompleteLayerUpload',
            'ecr-public:BatchCheckLayerAvailability'
          ],
          Effect: 'Allow',
          Resource: {
            'Fn::ImportValue': Match.anyValue()
          }
        }
      ]
    }
  });
});
