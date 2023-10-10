package org.opensearch.dataprepper.plugins.kafka.common.aws;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConnectionConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.util.function.Supplier;

/**
 * Standard implementation in Kafka plugins to get an {@link AwsCredentialsProvider}.
 * The key interface this implements is {@link Supplier}, supplying an {@link AwsCredentialsProvider}.
 * In general, you can provide the {@link Supplier} into class; just use this class when
 * constructing.
 */
public class AwsContext implements Supplier<AwsCredentialsProvider> {
    private final AwsConfig awsConfig;
    private final AwsCredentialsSupplier awsCredentialsSupplier;

    public AwsContext(KafkaConnectionConfig connectionConfig, AwsCredentialsSupplier awsCredentialsSupplier) {
        awsConfig = connectionConfig.getAwsConfig();
        this.awsCredentialsSupplier = awsCredentialsSupplier;
    }

    @Override
    public AwsCredentialsProvider get() {
        final AwsCredentialsOptions credentialsOptions;
        if(awsConfig != null) {
            credentialsOptions = awsConfig.toCredentialsOptions();
        } else {
            credentialsOptions = AwsCredentialsOptions.defaultOptions();
        }

        return awsCredentialsSupplier.getProvider(credentialsOptions);
    }

    public Region getRegion() {
        if(awsConfig != null && awsConfig.getRegion() != null) {
            return Region.of(awsConfig.getRegion());
        }
        return null;
    }
}
