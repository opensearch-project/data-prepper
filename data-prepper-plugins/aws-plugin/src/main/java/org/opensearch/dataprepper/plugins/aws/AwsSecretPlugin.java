package org.opensearch.dataprepper.plugins.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.annotations.DataPrepperExtensionPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.opensearch.dataprepper.model.plugin.PluginConfigPublisher;
import org.opensearch.dataprepper.model.plugin.PluginConfigValueTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@DataPrepperExtensionPlugin(modelType = AwsSecretPluginConfig.class, rootKeyJsonPath = "/aws/secrets",
        allowInPipelineConfigurations = true)
public class AwsSecretPlugin implements ExtensionPlugin {
    static final int PERIOD_IN_SECONDS = 60;
    private static final Logger LOG = LoggerFactory.getLogger(AwsSecretPlugin.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private ScheduledExecutorService scheduledExecutorService;
    private PluginConfigPublisher pluginConfigPublisher;
    private SecretsSupplier secretsSupplier;
    private final PluginConfigValueTranslator pluginConfigValueTranslator;

    @DataPrepperPluginConstructor
    public AwsSecretPlugin(final AwsSecretPluginConfig awsSecretPluginConfig) {
        if (awsSecretPluginConfig != null) {
            secretsSupplier = new AwsSecretsSupplier(awsSecretPluginConfig, OBJECT_MAPPER);
            this.pluginConfigPublisher = new AwsSecretsPluginConfigPublisher();
            pluginConfigValueTranslator = new AwsSecretsPluginConfigValueTranslator(secretsSupplier);
            scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
            submitSecretsRefreshJobs(awsSecretPluginConfig, secretsSupplier);
            Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
        } else {
            pluginConfigValueTranslator = null;
        }
    }

    @Override
    public void apply(final ExtensionPoints extensionPoints) {
        extensionPoints.addExtensionProvider(new AwsSecretsPluginConfigValueTranslatorExtensionProvider(pluginConfigValueTranslator));
        extensionPoints.addExtensionProvider(new AwsSecretsPluginConfigPublisherExtensionProvider(
                pluginConfigPublisher));
    }

    private void submitSecretsRefreshJobs(final AwsSecretPluginConfig awsSecretPluginConfig,
                                          final SecretsSupplier secretsSupplier) {
        awsSecretPluginConfig.getAwsSecretManagerConfigurationMap().forEach((key, value) -> {
            final SecretsRefreshJob secretsRefreshJob = new SecretsRefreshJob(
                    key, secretsSupplier, pluginConfigPublisher);
            final long period = value.getRefreshInterval().toSeconds();
            final long jitterDelay = ThreadLocalRandom.current().nextLong(60L);
            scheduledExecutorService.scheduleAtFixedRate(secretsRefreshJob, period + jitterDelay,
                    period, TimeUnit.SECONDS);
        });
    }

    void shutdown() {
        LOG.info("Shutting down secrets refreshing tasks.");
        scheduledExecutorService.shutdown();
        try {
            if (!scheduledExecutorService.awaitTermination(PERIOD_IN_SECONDS, TimeUnit.SECONDS)) {
                LOG.warn("Secrets refreshing tasks did not terminate in time, forcing termination");
                scheduledExecutorService.shutdownNow();
            }
        } catch (InterruptedException ex) {
            LOG.info("Encountered interruption terminating the secrets refreshing tasks execution, " +
                    "attempting to force the termination");
            scheduledExecutorService.shutdownNow();
        }
    }
}
