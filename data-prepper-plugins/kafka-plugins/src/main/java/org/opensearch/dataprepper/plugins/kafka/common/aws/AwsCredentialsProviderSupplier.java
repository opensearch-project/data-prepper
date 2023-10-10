package org.opensearch.dataprepper.plugins.kafka.common.aws;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConnectionConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.function.Supplier;

/**
 * Standard implementation in Kafka plugins to get an {@link AwsCredentialsProvider}.
 * The key interface this implements is {@link Supplier}, supplying an {@link AwsCredentialsProvider}.
 * In general, you can provide the {@link Supplier} into class; just use this class when
 * constructing.
 */
public class AwsCredentialsProviderSupplier implements Supplier<AwsCredentialsProvider> {
    private final KafkaConnectionConfig connectionConfig;
    private final AwsCredentialsSupplier awsCredentialsSupplier;

    public AwsCredentialsProviderSupplier(KafkaConnectionConfig connectionConfig, AwsCredentialsSupplier awsCredentialsSupplier) {
        this.connectionConfig = connectionConfig;
        this.awsCredentialsSupplier = awsCredentialsSupplier;
    }

    @Override
    public AwsCredentialsProvider get() {
        final AwsCredentialsOptions credentialsOptions;
        if(connectionConfig.getAwsConfig() != null) {
            credentialsOptions = connectionConfig.getAwsConfig().toCredentialsOptions();
        } else {
            credentialsOptions = AwsCredentialsOptions.defaultOptions();
        }

        return awsCredentialsSupplier.getProvider(credentialsOptions);
    }
}
