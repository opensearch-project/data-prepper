/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import {Duration, RemovalPolicy, Stack, StackProps} from 'aws-cdk-lib';
import {Construct} from 'constructs';
import {Role} from 'aws-cdk-lib/aws-iam';
import {Bucket} from 'aws-cdk-lib/aws-s3';

export interface S3SinkStackProps extends StackProps {
  readonly testingRole: Role;
}

/**
 * CDK stack that creates a common KMS key.
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

    this.bucket.grantWrite(props.testingRole)
  }
}

