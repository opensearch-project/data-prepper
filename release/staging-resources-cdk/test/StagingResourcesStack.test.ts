/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import {App} from 'aws-cdk-lib';
import {Capture, Match, Template} from 'aws-cdk-lib/assertions';
import {StagingResourcesStack} from '../lib/StagingResourcesStack';


let app: App;

beforeEach(() => {
  app = new App({
    context: {
      archivesBucketName: 'test-s3-bucket-name'
    }
  });
});

test('Creates an ECR Public Repository for Data Prepper', () => {
  const stackUnderTest = new StagingResourcesStack(app, 'TestStack');

  const template = Template.fromStack(stackUnderTest);

  template.hasResourceProperties('AWS::ECR::PublicRepository', {
    RepositoryName: 'data-prepper'
  });
});

test('Creates a CloudFront distribution with the bucket as the origin', () => {
  const stackUnderTest = new StagingResourcesStack(app, 'TestStack');

  const template = Template.fromStack(stackUnderTest);

  const domainNameCapture = new Capture();
  template.hasResourceProperties('AWS::CloudFront::Distribution', {
    DistributionConfig: {
      Origins: [
        {
          DomainName: domainNameCapture,
          S3OriginConfig: Match.anyValue()
        }
      ]
    }
  });

  const domainNameJson = JSON.stringify(domainNameCapture.asObject());
  expect(domainNameJson).toContain('test-s3-bucket-name')
});

test('Creates a CloudFront Origin Access Identity', () => {
  const stackUnderTest = new StagingResourcesStack(app, 'TestStack');

  const template = Template.fromStack(stackUnderTest);

  template.hasResourceProperties('AWS::CloudFront::CloudFrontOriginAccessIdentity', {});
});

test('Creates an S3 Bucket policy granting access to the CloudFront Origin Access Identity', () => {
  const stackUnderTest = new StagingResourcesStack(app, 'TestStack');

  const template = Template.fromStack(stackUnderTest);

  const bucketObjectsCapture = new Capture();
  template.hasResourceProperties('AWS::S3::BucketPolicy', {
    Bucket: 'test-s3-bucket-name',
    PolicyDocument: {
      Statement: [
        {
          Action: 's3:GetObject',
          Effect: 'Allow',
          Principal: {
            CanonicalUser: Match.anyValue()
          },
          Resource: bucketObjectsCapture
        }
      ]
    }
  });

  const allBucketsJson = JSON.stringify(bucketObjectsCapture.asObject());
  expect(allBucketsJson).toContain('test-s3-bucket-name/*')
});
