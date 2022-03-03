/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import {CfnOutput, RemovalPolicy, Stack, StackProps} from 'aws-cdk-lib';
import {Construct} from 'constructs';
import {BlockPublicAccess, Bucket} from 'aws-cdk-lib/aws-s3';

/**
 * This stack creates an S3 bucket for artifacts. It is distinct from
 * {@link StagingResourcesStack} to support using an existing bucket.
 */
export class ArtifactsBucketStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const archivesBucketName: string = scope.node.tryGetContext('archivesBucketName');

    const archivesBucket = new Bucket(this, 'StagingArtifactsBucket', {
      bucketName: archivesBucketName,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      removalPolicy: RemovalPolicy.RETAIN,
    });

    new CfnOutput(this, 'StagingArtifactsBucketName', {
      value: archivesBucket.bucketName
    });
  }
}
