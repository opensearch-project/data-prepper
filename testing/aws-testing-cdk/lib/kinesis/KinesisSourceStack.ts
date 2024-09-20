/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import {Stack, StackProps} from 'aws-cdk-lib';
import {Construct} from 'constructs';
import {Effect, Role, PolicyDocument, PolicyStatement, Policy} from 'aws-cdk-lib/aws-iam';

export interface KinesisSourceStackProps extends StackProps {
  readonly testingRole: Role;
}

/**
 * CDK stack that creates a Kinesis stream
 */
export class KinesisSourceStack extends Stack {

  constructor(scope: Construct, id: string, props: KinesisSourceStackProps) {
    super(scope, id, props);

    const kinesisPolicyDocument: PolicyDocument = new PolicyDocument({
      statements: [
        new PolicyStatement({
          actions: [
            'kinesis:ListStreams',
            'kinesis:ListStreamConsumers',
            'kinesis:ListShards',
            'kinesis:DescribeStream',
            'kinesis:GetRecords',
            'kinesis:GetResourcePolicy',
            'kinesis:SubscribeToShard',
            'kinesis:RegisterStreamConsumer',
            'kinesis:CreateStream',
            'kinesis:DeleteStream',
            'kinesis:PutRecord',
            'kinesis:PutRecords'
          ],
          effect: Effect.ALLOW,
          resources: ['*'],
        }),
      ],
    });

    const kinesisPolicy: Policy = new Policy(this, 'KinesisPolicy', {
      document: kinesisPolicyDocument
    });

    const dynamodbPolicyDocument: PolicyDocument = new PolicyDocument({
      statements: [
        new PolicyStatement({
          actions: [
            'dynamodb:DescribeTimeToLive',
            'dynamodb:CreateTable',
            'dynamodb:DescribeTable',
            'dynamodb:GetItem',
            'dynamodb:PutItem',
            'dynamodb:UpdateItem',
            'dynamodb:Scan',
            'dynamodb:Query',
            'dynamodb:DeleteItem',
            'dynamodb:updateContinuousBackups'
          ],
          effect: Effect.ALLOW,
          resources: ['*'],
        }),
      ],
    });

    const dynamodbPolicy: Policy = new Policy(this, 'DynamoDbPolicy', {
      document: dynamodbPolicyDocument
    });

    props.testingRole.attachInlinePolicy(kinesisPolicy);
    props.testingRole.attachInlinePolicy(dynamodbPolicy);

  }
}

