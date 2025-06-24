/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

import { Stack, StackProps } from 'aws-cdk-lib';
import { Role, ArnPrincipal, PolicyStatement, Effect, PolicyDocument } from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';

/**
 * This stack creates resources necessary for the OpenSearch CI server
 * to access the staging archives bucket by assuming a role in the staging
 * account.
 */
export class OpenSearchCIAccessStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const archivesBucketName: string = scope.node.tryGetContext('archivesBucketName');
    const ciAccountId: string = scope.node.tryGetContext('ciAccountId');

    new Role(this, 'OpenSearchCIAccessRole', {
      assumedBy: new ArnPrincipal(`arn:aws:iam::${ciAccountId}:root`),
      inlinePolicies: {
        S3Access: new PolicyDocument({
          statements: [new PolicyStatement({
            effect: Effect.ALLOW,
            actions: ['s3:GetObject', 's3:ListBucket', 's3:GetObjectAcl'],
            resources: [
              `arn:aws:s3:::${archivesBucketName}`,
              `arn:aws:s3:::${archivesBucketName}/*`
            ]
          })]
        })
      }
    });
  }
}
