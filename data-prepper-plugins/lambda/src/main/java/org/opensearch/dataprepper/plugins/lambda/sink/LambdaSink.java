/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.sink;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.BufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.DlqPushHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.LambdaClient;

import java.util.Collection;

@DataPrepperPlugin(name = "lambda", pluginType = Sink.class, pluginConfigurationType = LambdaSinkConfig.class)
public class LambdaSink extends AbstractSink<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(LambdaSink.class);
    private volatile boolean sinkInitialized;
    private final LambdaSinkService lambdaSinkService;
    private final BufferFactory bufferFactory;
    private static final String BUCKET = "bucket";
    private static final String KEY_PATH = "key_path_prefix";
    private DlqPushHandler dlqPushHandler = null;

    @DataPrepperPluginConstructor
    public LambdaSink(final PluginSetting pluginSetting,
                      final LambdaSinkConfig lambdaSinkConfig,
                      final PluginFactory pluginFactory,
                      final SinkContext sinkContext,
                      final AwsCredentialsSupplier awsCredentialsSupplier
    ) {
        super(pluginSetting);
        sinkInitialized = Boolean.FALSE;
        OutputCodecContext outputCodecContext = OutputCodecContext.fromSinkContext(sinkContext);
        LambdaClient lambdaClient = LambdaClientFactory.createLambdaClient(lambdaSinkConfig, awsCredentialsSupplier);
        if(lambdaSinkConfig.getDlqPluginSetting() != null) {
            this.dlqPushHandler = new DlqPushHandler(pluginFactory,
                    String.valueOf(lambdaSinkConfig.getDlqPluginSetting().get(BUCKET)),
                    lambdaSinkConfig.getDlqStsRoleARN()
                    , lambdaSinkConfig.getDlqStsRegion(),
                    String.valueOf(lambdaSinkConfig.getDlqPluginSetting().get(KEY_PATH)));
        }
        this.bufferFactory = new InMemoryBufferFactory();

        lambdaSinkService = new LambdaSinkService(lambdaClient,
                lambdaSinkConfig,
                pluginMetrics,
                pluginFactory,
                pluginSetting,
                outputCodecContext,
                awsCredentialsSupplier,
                dlqPushHandler,
                bufferFactory);

    }

    @Override
    public boolean isReady() {
        return sinkInitialized;
    }

    @Override
    public void doInitialize() {
        try {
            doInitializeInternal();
        } catch (InvalidPluginConfigurationException e) {
            LOG.error("Invalid plugin configuration, Hence failed to initialize s3-sink plugin.");
            this.shutdown();
            throw e;
        } catch (Exception e) {
            LOG.error("Failed to initialize lambda plugin.");
            this.shutdown();
            throw e;
        }
    }

    private void doInitializeInternal() {
        sinkInitialized = Boolean.TRUE;
    }

    /**
     * @param records Records to be output
     */
    @Override
    public void doOutput(final Collection<Record<Event>> records) {

        if (records.isEmpty()) {
            return;
        }
        lambdaSinkService.output(records);
    }    
}