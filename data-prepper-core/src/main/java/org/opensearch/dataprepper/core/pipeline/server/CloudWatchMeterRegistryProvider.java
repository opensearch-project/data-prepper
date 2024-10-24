/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline.server;

import io.micrometer.cloudwatch2.CloudWatchConfig;
import io.micrometer.cloudwatch2.CloudWatchMeterRegistry;
import io.micrometer.core.instrument.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

/**
 * Provides {@link CloudWatchMeterRegistry} that enables publishing metrics to AWS Cloudwatch. Registry
 * uses the default aws credentials (i.e. credentials from .aws directory;
 * refer https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/credentials.html#credentials-file-format).
 * {@link CloudWatchMeterRegistryProvider} also has a constructor with {@link CloudWatchAsyncClient} that will be used
 * for communication with Cloudwatch.
 */
public class CloudWatchMeterRegistryProvider {
    private static final String CLOUDWATCH_PROPERTIES = "cloudwatch.properties";
    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchMeterRegistryProvider.class);

    private final CloudWatchMeterRegistry cloudWatchMeterRegistry;

    public CloudWatchMeterRegistryProvider() {
        this(CLOUDWATCH_PROPERTIES, CloudWatchAsyncClient.create());
    }

    public CloudWatchMeterRegistryProvider(
            final String cloudWatchPropertiesFilePath,
            final CloudWatchAsyncClient cloudWatchAsyncClient) {
        final CloudWatchConfig cloudWatchConfig = createCloudWatchConfig(
                requireNonNull(cloudWatchPropertiesFilePath, "cloudWatchPropertiesFilePath must not be null"));
        this.cloudWatchMeterRegistry = new CloudWatchMeterRegistry(cloudWatchConfig, Clock.SYSTEM,
                requireNonNull(cloudWatchAsyncClient, "cloudWatchAsyncClient must not be null"));
    }

    /**
     * Returns the CloudWatchMeterRegistry created using the default aws credentials
     * @return CloudWatchMeterRegistry
     */
    public CloudWatchMeterRegistry getCloudWatchMeterRegistry() {
        return this.cloudWatchMeterRegistry;
    }

    /**
     * Returns CloudWatchConfig using the properties from {@link #CLOUDWATCH_PROPERTIES}
     */
    private CloudWatchConfig createCloudWatchConfig(final String cloudWatchPropertiesFilePath) {
        try (final InputStream inputStream = requireNonNull(getClass().getClassLoader()
                .getResourceAsStream(cloudWatchPropertiesFilePath))) {
            final Properties cloudwatchProperties = new Properties();
            cloudwatchProperties.load(inputStream);
            return cloudwatchProperties::getProperty;
        } catch (final IOException ex) {
            LOG.error("Encountered exception in creating CloudWatchConfig for CloudWatchMeterRegistry, " +
                    "Proceeding without metrics", ex);

            //If there is no registry attached, micrometer will make NoopMeters which are discarded.
            return null;
        }
    }
}
