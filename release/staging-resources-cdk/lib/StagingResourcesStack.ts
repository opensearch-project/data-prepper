/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import {Stack, StackProps} from 'aws-cdk-lib';
import {Construct} from 'constructs';
import {CfnPublicRepository} from 'aws-cdk-lib/aws-ecr'

export class StagingResourcesStack extends Stack {
  readonly dataPrepperEcrRepository: CfnPublicRepository;

  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    this.dataPrepperEcrRepository = new CfnPublicRepository(this, 'DataPrepperECRRepository', {
      repositoryName: 'data-prepper'
    });
  }
}
