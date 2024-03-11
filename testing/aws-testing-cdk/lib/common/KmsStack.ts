/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import {Stack, StackProps} from 'aws-cdk-lib';
import {Construct} from 'constructs';
import {Role} from 'aws-cdk-lib/aws-iam';
import {Key} from 'aws-cdk-lib/aws-kms';

export interface KmsStackProps extends StackProps {
  readonly testingRole: Role;
}

/**
 * CDK stack that creates a common KMS key.
 */
export class KmsStack extends Stack {
  readonly kmsKey: Key;

  constructor(scope: Construct, id: string, props: KmsStackProps) {
    super(scope, id, props);

    this.kmsKey = new Key(this, 'DataPrepperTestingKey', {
      alias: 'DataPrepperTesting',
      description: 'Shared KMS key for testing any Data Prepper features that use KMS.'
    });

    this.kmsKey.grantEncryptDecrypt(props.testingRole)
  }
}

