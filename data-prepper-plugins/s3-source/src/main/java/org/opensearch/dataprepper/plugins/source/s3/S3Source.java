/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.UsesSourceCoordination;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanScanOptions;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3SelectCSVOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3SelectJsonOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3SelectOptions;
import org.opensearch.dataprepper.plugins.source.s3.ownership.BucketOwnerProvider;
import org.opensearch.dataprepper.plugins.source.s3.ownership.ConfigBucketOwnerProviderFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.s3.model.CompressionType;

import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;

@DataPrepperPlugin(name = "s3", pluginType = Source.class, pluginConfigurationType = S3SourceConfig.class)
public class S3Source implements Source<Record<Event>>, UsesSourceCoordination {

    private final PluginMetrics pluginMetrics;
    private final S3SourceConfig s3SourceConfig;
    private SqsService sqsService;
    private S3ScanService s3ScanService;
    private final PluginFactory pluginFactory;
    private final Optional<S3ScanScanOptions> s3ScanScanOptional;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final AwsCredentialsSupplier awsCredentialsSupplier;
    private final boolean acknowledgementsEnabled;
    private SourceCoordinator<S3SourceProgressState> sourceCoordinator;


    @DataPrepperPluginConstructor
    public S3Source(
            final PluginMetrics pluginMetrics,
            final S3SourceConfig s3SourceConfig,
            final PluginFactory pluginFactory,
            final AcknowledgementSetManager acknowledgementSetManager,
            final AwsCredentialsSupplier awsCredentialsSupplier) {
        this.pluginMetrics = pluginMetrics;
        this.s3SourceConfig = s3SourceConfig;
        this.pluginFactory = pluginFactory;
        this.s3ScanScanOptional = Optional.ofNullable(s3SourceConfig.getS3ScanScanOptions());
        this.acknowledgementsEnabled = s3SourceConfig.getAcknowledgements();
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.awsCredentialsSupplier = awsCredentialsSupplier;
    }

    @Override
    public boolean areAcknowledgementsEnabled() {
        return acknowledgementsEnabled;
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }
        final AwsAuthenticationAdapter awsAuthenticationAdapter = new AwsAuthenticationAdapter(awsCredentialsSupplier, s3SourceConfig);
        final AwsCredentialsProvider credentialsProvider = awsAuthenticationAdapter.getCredentialsProvider();
        final ConfigBucketOwnerProviderFactory configBucketOwnerProviderFactory = new ConfigBucketOwnerProviderFactory(credentialsProvider);
        final BucketOwnerProvider bucketOwnerProvider = configBucketOwnerProviderFactory.createBucketOwnerProvider(s3SourceConfig);
        Optional<S3SelectOptions> s3SelectOptional = Optional.ofNullable(s3SourceConfig.getS3SelectOptions());
        S3ObjectPluginMetrics s3ObjectPluginMetrics = new S3ObjectPluginMetrics(pluginMetrics);

        S3ClientBuilderFactory s3ClientBuilderFactory = new S3ClientBuilderFactory(s3SourceConfig, credentialsProvider);
        final S3ObjectHandler s3Handler;
        final S3ObjectRequest.Builder s3ObjectRequestBuilder = new S3ObjectRequest.Builder(buffer, s3SourceConfig.getNumberOfRecordsToAccumulate(),
                s3SourceConfig.getBufferTimeout(), s3ObjectPluginMetrics);
        final BiConsumer<Event, S3ObjectReference> eventMetadataModifier = new EventMetadataModifier(
                s3SourceConfig.getMetadataRootKey(), s3SourceConfig.isDeleteS3MetadataInEvent());
        final S3ObjectDeleteWorker s3ObjectDeleteWorker = new S3ObjectDeleteWorker(s3ClientBuilderFactory.getS3Client(), pluginMetrics);

        if (s3SelectOptional.isPresent()) {
            S3SelectCSVOption csvOption = (s3SelectOptional.get().getS3SelectCSVOption() != null) ?
                    s3SelectOptional.get().getS3SelectCSVOption() : new S3SelectCSVOption();
            S3SelectJsonOption jsonOption = (s3SelectOptional.get().getS3SelectJsonOption() != null) ?
                    s3SelectOptional.get().getS3SelectJsonOption() : new S3SelectJsonOption();
            S3ObjectRequest s3ObjectRequest = s3ObjectRequestBuilder.expression(s3SelectOptional.get().getExpression())
                    .serializationFormatOption(s3SelectOptional.get().getS3SelectSerializationFormatOption())
                    .s3AsyncClient(s3ClientBuilderFactory.getS3AsyncClient())
                    .eventConsumer(eventMetadataModifier)
                    .bucketOwnerProvider(bucketOwnerProvider)
                    .s3SelectCSVOption(csvOption)
                    .s3SelectJsonOption(jsonOption)
                    .expressionType(s3SelectOptional.get().getExpressionType())
                    .compressionType(CompressionType.valueOf(s3SelectOptional.get().getCompressionType().toUpperCase()))
                    .s3SelectResponseHandlerFactory(new S3SelectResponseHandlerFactory()).build();
            s3Handler = new S3SelectObjectWorker(s3ObjectRequest);
        } else {
            final PluginModel codecConfiguration = s3SourceConfig.getCodec();
            final PluginSetting codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(), codecConfiguration.getPluginSettings());
            final InputCodec codec = pluginFactory.loadPlugin(InputCodec.class, codecPluginSettings);
            final S3ObjectRequest s3ObjectRequest = s3ObjectRequestBuilder
                    .bucketOwnerProvider(bucketOwnerProvider)
                    .codec(codec)
                    .eventConsumer(eventMetadataModifier)
                    .s3Client(s3ClientBuilderFactory.getS3Client())
                    .compressionOption(s3SourceConfig.getCompression())
                    .build();
            s3Handler = new S3ObjectWorker(s3ObjectRequest);
        }
        if(Objects.nonNull(s3SourceConfig.getSqsOptions())) {
            final S3Service s3Service = new S3Service(s3Handler);
            sqsService = new SqsService(acknowledgementSetManager, s3SourceConfig, s3Service, pluginMetrics, credentialsProvider);
            sqsService.start();
        }
        if(s3ScanScanOptional.isPresent()) {
            s3ScanService = new S3ScanService(s3SourceConfig, s3ClientBuilderFactory, s3Handler, bucketOwnerProvider, sourceCoordinator, acknowledgementSetManager, s3ObjectDeleteWorker, pluginMetrics);
            s3ScanService.start();
        }
    }

    @Override
    public void stop() {

        if (Objects.nonNull(sqsService)) {
            sqsService.stop();
        }

        if (Objects.nonNull(s3ScanService) && Objects.nonNull(sourceCoordinator)) {
            s3ScanService.stop();
        }
    }

    @Override
    public <T> void setSourceCoordinator(final SourceCoordinator<T> sourceCoordinator) {
        this.sourceCoordinator = (SourceCoordinator<S3SourceProgressState>) sourceCoordinator;
    }

    @Override
    public Class<?> getPartitionProgressStateClass() {
        return S3SourceProgressState.class;
    }
}
