/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import {App} from 'aws-cdk-lib';
import {Template} from 'aws-cdk-lib/assertions';
import {ArchivesBucketStack} from '../lib/ArchivesBucketStack';


let app: App;

beforeEach(() => {
  app = new App({
    context: {
      archivesBucketName: 'test-s3-bucket-name'
    }
  });
});

test('Creates an S3 Bucket', () => {
  const stackUnderTest = new ArchivesBucketStack(app, 'TestStack');

  const template = Template.fromStack(stackUnderTest);

  template.hasResourceProperties('AWS::S3::Bucket', {
    BucketName: 'test-s3-bucket-name',
    PublicAccessBlockConfiguration: {
      BlockPublicAcls: true,
      BlockPublicPolicy: true,
      IgnorePublicAcls: true,
      RestrictPublicBuckets: true
    }
  });

  template.hasResource('AWS::S3::Bucket', {
    UpdateReplacePolicy: 'Retain',
    DeletionPolicy: 'Retain',
  })
});

test('Configures the S3 Bucket with a retain policy', () => {
  const stackUnderTest = new ArchivesBucketStack(app, 'TestStack');

  const template = Template.fromStack(stackUnderTest);

  template.hasResource('AWS::S3::Bucket', {
    UpdateReplacePolicy: 'Retain',
    DeletionPolicy: 'Retain',
  })
});
