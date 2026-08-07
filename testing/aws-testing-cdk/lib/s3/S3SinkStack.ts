/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

import {CfnOutput, Duration, RemovalPolicy, Stack, StackProps} from 'aws-cdk-lib';
import {Construct} from 'constructs';
import {Role} from 'aws-cdk-lib/aws-iam';
import {Bucket} from 'aws-cdk-lib/aws-s3';

export interface S3SinkStackProps extends StackProps {
  readonly testingRole: Role;
}

/**
 * CDK stack for resources needed by the S3 sink integration tests.
 */
export class S3SinkStack extends Stack {
  readonly bucket: Bucket;

  constructor(scope: Construct, id: string, props: S3SinkStackProps) {
    super(scope, id, props);

    this.bucket = new Bucket(this, 'DataPrepperSinkTest', {
      removalPolicy: RemovalPolicy.DESTROY,
      lifecycleRules: [
        {
          expiration: Duration.days(10)
        }
      ]
    });

    this.bucket.grantReadWrite(props.testingRole)

    new CfnOutput(this, 'BucketName', {
      value: this.bucket.bucketName,
      exportName: 'DataPrepperAwsTesting-S3SinkBucketName',
    });
  }
}

