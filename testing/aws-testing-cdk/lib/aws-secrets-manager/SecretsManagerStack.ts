import {SecretValue, Stack, StackProps} from 'aws-cdk-lib';
import {Role} from 'aws-cdk-lib/aws-iam';
import {Construct} from 'constructs';
import {Secret} from 'aws-cdk-lib/aws-secretsmanager';

export interface SecretsManagerStackProps extends StackProps {
  readonly testingRole: Role;
}

export class SecretsManagerStack extends Stack {
  readonly openSearchSinkSecrets : Secret;

  constructor(scope: Construct, id: string, props: SecretsManagerStackProps) {
    super(scope, id, props);

    this.openSearchSinkSecrets = new Secret(this, 'OpenSearchSinkSecrets', {
      secretName: 'opensearch-sink-basic-credentials',
      secretObjectValue: {
        // TODO: pass in secret values as args?
        username: SecretValue.unsafePlainText('admin'),
        password: SecretValue.unsafePlainText('admin'),
      }
    });
    this.openSearchSinkSecrets.grantRead(props.testingRole);
  }
}