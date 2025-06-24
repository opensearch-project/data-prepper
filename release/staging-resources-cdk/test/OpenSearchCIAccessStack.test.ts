/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

import {App} from 'aws-cdk-lib';
import {Template} from 'aws-cdk-lib/assertions';
import {OpenSearchCIAccessStack} from '../lib/OpenSearchCIAccessStack';

let app: App;

beforeEach(() => {
  app = new App({
    context: {
      archivesBucketName: 'test-archives-bucket',
      ciAccountId: '123456789012'
    }
  });
});

test('Creates IAM role with correct assume role policy', () => {
  const stackUnderTest = new OpenSearchCIAccessStack(app, 'TestStack');

  const template = Template.fromStack(stackUnderTest);

  template.hasResourceProperties('AWS::IAM::Role', {
    AssumeRolePolicyDocument: {
      Statement: [
        {
          Effect: 'Allow',
          Principal: {
            AWS: 'arn:aws:iam::123456789012:role/OpenSearch-CI-MainNodeRole'
          },
          Action: 'sts:AssumeRole'
        },
        {
          Effect: 'Allow',
          Principal: {
            AWS: 'arn:aws:iam::123456789012:role/OpenSearch-CI-AgentNodeRole'
          },
          Action: 'sts:AssumeRole'
        }
      ]
    }
  });
});

test('Creates IAM role with S3 access policy', () => {
  const stackUnderTest = new OpenSearchCIAccessStack(app, 'TestStack');

  const template = Template.fromStack(stackUnderTest);

  template.hasResourceProperties('AWS::IAM::Role', {
    Policies: [{
      PolicyName: 'S3Access',
      PolicyDocument: {
        Statement: [{
          Effect: 'Allow',
          Action: ['s3:GetObject', 's3:ListBucket', 's3:GetObjectAcl'],
          Resource: [
            'arn:aws:s3:::test-archives-bucket',
            'arn:aws:s3:::test-archives-bucket/*'
          ]
        }]
      }
    }]
  });
});
