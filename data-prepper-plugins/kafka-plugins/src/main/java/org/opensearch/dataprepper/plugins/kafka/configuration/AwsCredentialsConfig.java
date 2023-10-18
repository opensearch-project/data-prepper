package org.opensearch.dataprepper.plugins.kafka.configuration;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;

public interface AwsCredentialsConfig {
    String getRegion();
    String getStsRoleArn();

    AwsCredentialsOptions toCredentialsOptions();
}
