/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.configuration.PluginModel;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.plugin.PluginFactory;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.source.codec.Codec;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import org.opensearch.dataprepper.plugins.source.ownership.ConfigBucketOwnerProviderFactory;

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

        final S3Service s3Service = new S3Service(s3SourceConfig, buffer, codec, pluginMetrics, bucketOwnerProvider);
        sqsService = new SqsService(s3SourceConfig, s3Service, pluginMetrics);

        sqsService.start();
    }

    @Override
    public void stop() {
        sqsService.stop();
    }
}
