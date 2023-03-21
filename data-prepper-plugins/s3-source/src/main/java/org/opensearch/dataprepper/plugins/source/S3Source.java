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
import org.opensearch.dataprepper.plugins.source.configuration.S3ScanScanOptions;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import org.opensearch.dataprepper.plugins.source.ownership.ConfigBucketOwnerProviderFactory;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectOptions;
import software.amazon.awssdk.services.s3.model.CompressionType;
import software.amazon.awssdk.services.s3.model.FileHeaderInfo;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.BiConsumer;

@DataPrepperPlugin(name = "s3", pluginType = Source.class, pluginConfigurationType = S3SourceConfig.class)
public class S3Source implements Source<Record<Event>> {

    private final PluginMetrics pluginMetrics;
    private final S3SourceConfig s3SourceConfig;
    private SqsService sqsService;
    private final PluginFactory pluginFactory;
    private final Optional<S3ScanScanOptions> s3ScanScanOptional;

    @DataPrepperPluginConstructor
    public S3Source(PluginMetrics pluginMetrics, final S3SourceConfig s3SourceConfig, final PluginFactory pluginFactory) {
        this.pluginMetrics = pluginMetrics;
        this.s3SourceConfig = s3SourceConfig;
        this.pluginFactory = pluginFactory;
        this.s3ScanScanOptional = Optional.ofNullable(s3SourceConfig.getS3ScanScanOptions());
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
        if (s3SelectOptional.isPresent()) {
            S3ObjectRequest s3ObjectRequest = s3ObjectRequestBuilder.queryStatement(s3SelectOptional.get().getQueryStatement())
                    .serializationFormatOption(s3SelectOptional.get().getS3SelectSerializationFormatOption())
                    .s3AsyncClient(s3ClientBuilderFactory.getS3AsyncClient()).
                    fileHeaderInfo(FileHeaderInfo.valueOf(s3SelectOptional.get().getCsvFileHeaderInfo().toUpperCase()))
                    .compressionType(CompressionType.valueOf(s3SourceConfig.getCompression().name()))
                    .s3SelectResponseHandler(new S3SelectResponseHandler()).build();
            s3Handler = new S3SelectObjectWorker(s3ObjectRequest);
        } else {
            if(s3SourceConfig.getCodec() == null)
                throw new NoSuchElementException("codec is required in pipeline yaml configuration");
            final PluginModel codecConfiguration = s3SourceConfig.getCodec();
            final PluginSetting codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(), codecConfiguration.getPluginSettings());
            final Codec codec = pluginFactory.loadPlugin(Codec.class, codecPluginSettings);
            final BiConsumer<Event, S3ObjectReference> eventMetadataModifier = new EventMetadataModifier(
                    s3SourceConfig.getMetadataRootKey());
            final S3ObjectRequest s3ObjectRequest = s3ObjectRequestBuilder.bucketOwnerProvider(bucketOwnerProvider)
                    .eventConsumer(eventMetadataModifier).codec(codec).s3Client(s3ClientBuilderFactory.getS3Client())
                    .compressionEngine(s3SourceConfig.getCompression().getEngine()).build();
            s3Handler = new S3ObjectWorker(s3ObjectRequest);
        }
        final S3Service s3Service = new S3Service(s3Handler);
        sqsService = new SqsService(s3SourceConfig, s3Service, pluginMetrics);

        sqsService.start();
        final S3ScanService s3ScanService;
        if(s3ScanScanOptional.isPresent()) {
            s3ScanService = new S3ScanService(s3SourceConfig,buffer,bucketOwnerProvider,new EventMetadataModifier(
                    s3SourceConfig.getMetadataRootKey()),s3ObjectPluginMetrics,pluginFactory,s3ClientBuilderFactory);
            s3ScanService.start();
        }
    }

    @Override
    public void stop() {
        sqsService.stop();
    }
}
