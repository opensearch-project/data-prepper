/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.codec.parquet.ParquetOutputCodec;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.CodecBufferFactory;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.CompressionBufferFactory;
import org.opensearch.dataprepper.plugins.sink.s3.codec.BufferedCodec;
import org.opensearch.dataprepper.plugins.sink.s3.codec.CodecFactory;
import org.opensearch.dataprepper.plugins.sink.s3.compression.CompressionEngine;
import org.opensearch.dataprepper.plugins.sink.s3.compression.CompressionOption;
import org.opensearch.dataprepper.plugins.sink.s3.grouping.S3GroupIdentifierFactory;
import org.opensearch.dataprepper.plugins.sink.s3.grouping.S3GroupManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Duration;
import java.util.Collection;

/**
 * Implementation class of s3-sink plugin. It is responsible for receive the collection of
 * {@link Event} and upload to amazon s3 based on thresholds configured.
 */
@DataPrepperPlugin(name = "s3", pluginType = Sink.class, pluginConfigurationType = S3SinkConfig.class)
public class S3Sink extends AbstractSink<Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(S3Sink.class);

    private static final Duration RETRY_FLUSH_BACKOFF = Duration.ofSeconds(5);
    private final S3SinkConfig s3SinkConfig;
    private volatile boolean sinkInitialized;
    private final S3SinkService s3SinkService;
    private final BufferFactory bufferFactory;
    private final SinkContext sinkContext;

    /**
     * @param pluginSetting dp plugin settings.
     * @param s3SinkConfig  s3 sink configurations.
     * @param pluginFactory dp plugin factory.
     */
    @DataPrepperPluginConstructor
    public S3Sink(final PluginSetting pluginSetting,
                  final S3SinkConfig s3SinkConfig,
                  final PluginFactory pluginFactory,
                  final SinkContext sinkContext,
                  final AwsCredentialsSupplier awsCredentialsSupplier,
                  final ExpressionEvaluator expressionEvaluator) {
        super(pluginSetting);
        this.s3SinkConfig = s3SinkConfig;
        this.sinkContext = sinkContext;
        final PluginModel codecConfiguration = s3SinkConfig.getCodec();
        final CodecFactory codecFactory = new CodecFactory(pluginFactory, codecConfiguration);

        final PluginSetting codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(),
                codecConfiguration.getPluginSettings());
        final OutputCodec testCodec = pluginFactory.loadPlugin(OutputCodec.class, codecPluginSettings);
        sinkInitialized = Boolean.FALSE;

        final S3Client s3Client = ClientFactory.createS3Client(s3SinkConfig, awsCredentialsSupplier);
        BufferFactory innerBufferFactory = s3SinkConfig.getBufferType().getBufferFactory();
        if(testCodec instanceof ParquetOutputCodec && s3SinkConfig.getBufferType() != BufferTypeOptions.INMEMORY) {
            throw new InvalidPluginConfigurationException("The Parquet sink codec is an in_memory buffer only.");
        }
        if(testCodec instanceof BufferedCodec) {
            innerBufferFactory = new CodecBufferFactory(innerBufferFactory, (BufferedCodec) testCodec);
        }
        CompressionOption compressionOption = s3SinkConfig.getCompression();
        final CompressionEngine compressionEngine = compressionOption.getCompressionEngine();
        bufferFactory = new CompressionBufferFactory(innerBufferFactory, compressionEngine, testCodec);

        ExtensionProvider extensionProvider = StandardExtensionProvider.create(testCodec, compressionOption);
        KeyGenerator keyGenerator = new KeyGenerator(s3SinkConfig, extensionProvider, expressionEvaluator);

        if (s3SinkConfig.getObjectKeyOptions().getPathPrefix() != null &&
            !expressionEvaluator.isValidFormatExpression(s3SinkConfig.getObjectKeyOptions().getPathPrefix())) {
            throw new InvalidPluginConfigurationException("path_prefix is not a valid format expression");
        }

        if (s3SinkConfig.getObjectKeyOptions().getNamePattern() != null &&
                !expressionEvaluator.isValidFormatExpression(s3SinkConfig.getObjectKeyOptions().getNamePattern())) {
            throw new InvalidPluginConfigurationException("name_pattern is not a valid format expression");
        }

        if (s3SinkConfig.getBucketName() != null &&
                !expressionEvaluator.isValidFormatExpression(s3SinkConfig.getBucketName())) {
            throw new InvalidPluginConfigurationException("bucket name is not a valid format expression");
        }

        S3OutputCodecContext s3OutputCodecContext = new S3OutputCodecContext(OutputCodecContext.fromSinkContext(sinkContext), compressionOption);

        testCodec.validateAgainstCodecContext(s3OutputCodecContext);

        final S3GroupIdentifierFactory s3GroupIdentifierFactory = new S3GroupIdentifierFactory(keyGenerator, expressionEvaluator, s3SinkConfig);
        final S3GroupManager s3GroupManager = new S3GroupManager(s3SinkConfig, s3GroupIdentifierFactory, bufferFactory, codecFactory, s3Client);


        s3SinkService = new S3SinkService(s3SinkConfig, s3OutputCodecContext, s3Client, keyGenerator, RETRY_FLUSH_BACKOFF, pluginMetrics, s3GroupManager);
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
            LOG.error("Failed to initialize s3-sink plugin.");
            this.shutdown();
            throw e;
        }
    }

    /**
     * Initialize {@link S3SinkService}
     */
    private void doInitializeInternal() {
        sinkInitialized = Boolean.TRUE;
    }

    /**
     * @param records Records to be output
     */
    @Override
    public void doOutput(final Collection<Record<Event>> records) {
        s3SinkService.output(records);
    }
}
