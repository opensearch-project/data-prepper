/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.source.codec.Codec;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import org.opensearch.dataprepper.plugins.source.ownership.ConfigBucketOwnerProviderFactory;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectOptions;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectCSVOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectJsonOption;
import software.amazon.awssdk.services.s3.model.CompressionType;

import java.util.Optional;
import java.util.function.BiConsumer;

@DataPrepperPlugin(name = "s3", pluginType = Source.class, pluginConfigurationType = S3SourceConfig.class)
public class S3Source implements Source<Record<Event>> {

    private final PluginMetrics pluginMetrics;
    private final S3SourceConfig s3SourceConfig;
    private SqsService sqsService;
    private final PluginFactory pluginFactory;

    @DataPrepperPluginConstructor
    public S3Source(PluginMetrics pluginMetrics, final S3SourceConfig s3SourceConfig, final PluginFactory pluginFactory) {
        this.pluginMetrics = pluginMetrics;
        this.s3SourceConfig = s3SourceConfig;
        this.pluginFactory = pluginFactory;
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }

        final ConfigBucketOwnerProviderFactory configBucketOwnerProviderFactory = new ConfigBucketOwnerProviderFactory();
        final BucketOwnerProvider bucketOwnerProvider = configBucketOwnerProviderFactory.createBucketOwnerProvider(s3SourceConfig);

        Optional<S3SelectOptions> s3SelectOptional = Optional.ofNullable(s3SourceConfig.getS3SelectOptions());
        S3ObjectPluginMetrics s3ObjectPluginMetrics = new S3ObjectPluginMetrics(pluginMetrics);

        S3ClientBuilderFactory s3ClientBuilderFactory = new S3ClientBuilderFactory(s3SourceConfig);
        final S3ObjectHandler s3Handler;
        final S3ObjectRequest.Builder s3ObjectRequestBuilder = new S3ObjectRequest.Builder(buffer, s3SourceConfig.getNumberOfRecordsToAccumulate(),
                s3SourceConfig.getBufferTimeout(), s3ObjectPluginMetrics);
        final BiConsumer<Event, S3ObjectReference> eventMetadataModifier = new EventMetadataModifier(
                s3SourceConfig.getMetadataRootKey());
        if (s3SelectOptional.isPresent()) {
            S3SelectCSVOption csvOption = (s3SelectOptional.get().getS3SelectCSVOption() != null) ?
                    s3SelectOptional.get().getS3SelectCSVOption() : new S3SelectCSVOption();
            S3SelectJsonOption jsonOption = (s3SelectOptional.get().getS3SelectJsonOption() != null) ?
                    s3SelectOptional.get().getS3SelectJsonOption() : new S3SelectJsonOption();
            S3ObjectRequest s3ObjectRequest = s3ObjectRequestBuilder.expression(s3SelectOptional.get().getExpression())
                    .serializationFormatOption(s3SelectOptional.get().getS3SelectSerializationFormatOption())
                    .s3AsyncClient(s3ClientBuilderFactory.getS3AsyncClient()).eventConsumer(eventMetadataModifier).
                    s3SelectCSVOption(csvOption).s3SelectJsonOption(jsonOption)
                    .expressionType(s3SelectOptional.get().getExpressionType())
                    .compressionType(CompressionType.valueOf(s3SelectOptional.get().getCompressionType().toUpperCase()))
                    .s3SelectResponseHandler(new S3SelectResponseHandler()).build();
            s3Handler = new S3SelectObjectWorker(s3ObjectRequest);
        } else {
            final PluginModel codecConfiguration = s3SourceConfig.getCodec();
            final PluginSetting codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(), codecConfiguration.getPluginSettings());
            final Codec codec = pluginFactory.loadPlugin(Codec.class, codecPluginSettings);
            final S3ObjectRequest s3ObjectRequest = s3ObjectRequestBuilder
                    .bucketOwnerProvider(bucketOwnerProvider)
                    .codec(codec)
                    .eventConsumer(eventMetadataModifier)
                    .s3Client(s3ClientBuilderFactory.getS3Client())
                    .compressionEngine(s3SourceConfig.getCompression().getEngine())
                    .build();
            s3Handler = new S3ObjectWorker(s3ObjectRequest);
        }
        final S3Service s3Service = new S3Service(s3Handler);
        sqsService = new SqsService(s3SourceConfig, s3Service, pluginMetrics);

        sqsService.start();
    }

    @Override
    public void stop() {
        sqsService.stop();
    }
}
