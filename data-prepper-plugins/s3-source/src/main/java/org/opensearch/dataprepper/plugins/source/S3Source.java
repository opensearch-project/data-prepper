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
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectOptions;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import org.opensearch.dataprepper.plugins.source.ownership.ConfigBucketOwnerProviderFactory;

import java.util.Optional;
import java.util.function.BiConsumer;

@DataPrepperPlugin(name = "s3", pluginType = Source.class, pluginConfigurationType = S3SourceConfig.class)
public class S3Source implements Source<Record<Event>> {

    private final PluginMetrics pluginMetrics;
    private final S3SourceConfig s3SourceConfig;
    private final Codec codec;

    private SqsService sqsService;

    @DataPrepperPluginConstructor
    public S3Source(PluginMetrics pluginMetrics, final S3SourceConfig s3SourceConfig, final PluginFactory pluginFactory) {
        this.pluginMetrics = pluginMetrics;
        this.s3SourceConfig = s3SourceConfig;
        final PluginModel codecConfiguration = s3SourceConfig.getCodec();

        final PluginSetting codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(), codecConfiguration.getPluginSettings());
        codec = pluginFactory.loadPlugin(Codec.class, codecPluginSettings);
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }

        final ConfigBucketOwnerProviderFactory configBucketOwnerProviderFactory = new ConfigBucketOwnerProviderFactory();
        final BucketOwnerProvider bucketOwnerProvider = configBucketOwnerProviderFactory.createBucketOwnerProvider(s3SourceConfig);

        Optional<S3SelectOptions> s3SelectOptional = Optional.ofNullable(s3SourceConfig.getS3SelectOptions());

        final S3ObjectHandler s3Handler;
		if (s3SelectOptional.isPresent()) {
			s3Handler = new S3SelectObjectWorker(buffer, s3SourceConfig.getNumberOfRecordsToAccumulate(),
					s3SourceConfig.getBufferTimeout(), pluginMetrics, s3SelectOptional.get().getQueryStatement(),
					s3SelectOptional.get().getS3SelectSerializationFormatOption().name());
		} else {
			final BiConsumer<Event, S3ObjectReference> eventMetadataModifier = new EventMetadataModifier(
					s3SourceConfig.getMetadataRootKey());
			s3Handler = new S3ObjectWorker(buffer, s3SourceConfig.getCompression().getEngine(), codec,
					bucketOwnerProvider, s3SourceConfig.getBufferTimeout(),
					s3SourceConfig.getNumberOfRecordsToAccumulate(), eventMetadataModifier, pluginMetrics);
		}
        final S3Service s3Service = new S3Service(s3Handler,s3SourceConfig);

        sqsService = new SqsService(s3SourceConfig, s3Service, pluginMetrics);

        sqsService.start();
    }

    @Override
    public void stop() {
        sqsService.stop();
    }
}
