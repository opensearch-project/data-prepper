/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import {CfnOutput, Stack, StackProps} from 'aws-cdk-lib';
import {Construct} from 'constructs';
import {CfnPublicRepository} from 'aws-cdk-lib/aws-ecr'
import {CloudFrontWebDistribution, OriginAccessIdentity} from 'aws-cdk-lib/aws-cloudfront';
import {Bucket, BucketPolicy} from 'aws-cdk-lib/aws-s3';
import {PolicyStatement} from 'aws-cdk-lib/aws-iam';

/**
 * This stack creates the resources necessary for the staging environment used
 * in the Data Prepper release process. It excludes related IAM resources.
 */
export class StagingResourcesStack extends Stack {
  readonly dataPrepperEcrRepository: CfnPublicRepository;

  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    this.dataPrepperEcrRepository = new CfnPublicRepository(this, 'DataPrepperECRRepository', {
      repositoryName: 'data-prepper'
    });

    const archivesBucketName: string = scope.node.tryGetContext('archivesBucketName');

    const archivesBucket = Bucket.fromBucketName(this, 'StagingBucket', archivesBucketName);
    const originAccessIdentity = new OriginAccessIdentity(this, 'StagingOriginAccessIdentity');

    const cloudFrontDistribution = new CloudFrontWebDistribution(this, 'StagingCloudFrontDistribution', {
      originConfigs: [
        {
          s3OriginSource: {
            s3BucketSource: archivesBucket,
            originAccessIdentity: originAccessIdentity
          },
          behaviors: [
            {
              isDefaultBehavior: true
            }
          ]
        }
      ]
    });

    const policyStatement = new PolicyStatement({
      actions: [
        's3:GetObject'
      ],
      resources: [
        archivesBucket.arnForObjects('*')
      ],
      principals: [
        originAccessIdentity.grantPrincipal
      ]
    });
    const bucketPolicy = new BucketPolicy(this, 'StagingDistributionOriginAccess', {
      bucket: archivesBucket
    });
    bucketPolicy.document.addStatements(policyStatement);


    new CfnOutput(this, 'StagingDomainName', {
      value: cloudFrontDistribution.distributionDomainName
    });

    new CfnOutput(this, 'StagingCloudFrontDistributionId', {
      value: cloudFrontDistribution.distributionId
    });
  }
}
