/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

import { Stack, StackProps } from 'aws-cdk-lib';
import { Role, ArnPrincipal, PolicyStatement, Effect, PolicyDocument, CompositePrincipal } from 'aws-cdk-lib/aws-iam';
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
    const ciAccountIds: string = scope.node.tryGetContext('ciAccountIds');

    const trustedAccountIds = ciAccountIds.split(',')
      .map(id => id.trim())
      .filter(id => id.length > 0);
    
    if (trustedAccountIds.length === 0) {
      throw new Error('At least one account ID must be provided in ciAccountIds');
    }
    
    const principals = [];
    
    for (const accountId of trustedAccountIds) {
      principals.push(
        new ArnPrincipal(`arn:aws:iam::${accountId}:role/OpenSearch-CI-MainNodeRole`),
        new ArnPrincipal(`arn:aws:iam::${accountId}:role/OpenSearch-CI-AgentNodeRole`)
      );
    }

    new Role(this, 'OpenSearchCIAccessRole', {
      assumedBy: new CompositePrincipal(...principals),
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
