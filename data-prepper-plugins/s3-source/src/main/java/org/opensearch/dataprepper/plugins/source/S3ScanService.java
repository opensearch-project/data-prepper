/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.codec.Codec;
import org.opensearch.dataprepper.plugins.source.configuration.CompressionOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3ScanBucketOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3ScanBucketOptions;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectOptions;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectSerializationFormatOption;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import software.amazon.awssdk.services.s3.model.FileHeaderInfo;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.BiConsumer;

/**
 * Class responsible for taking an {@link S3SourceConfig} and creating all the necessary {@link ScanOptionsBuilder}
 * objects sand span a thread {@link S3SelectObjectWorker}
 */
public class S3ScanService {
    private final List<S3ScanBucketOptions> s3ScanBucketOptions;
    private final Buffer<Record<Event>> buffer;
    private final BucketOwnerProvider bucketOwnerProvider;
    private final Duration bufferTimeout;
    private final int numberOfRecordsToAccumulate;
    private final BiConsumer<Event, S3ObjectReference> eventConsumer;
    private final S3ObjectPluginMetrics s3ObjectPluginMetrics;
    private final PluginFactory pluginFactory;
    private final S3ClientBuilderFactory s3ClientBuilderFactory;
    private final String endTime;
    private final String range;
    private CompressionOption compressionOption;

    public S3ScanService(final S3SourceConfig s3SourceConfig,
                         final Buffer<Record<Event>> buffer,
                         final BucketOwnerProvider bucketOwnerProvider,
                         final BiConsumer<Event, S3ObjectReference> eventConsumer,
                         final S3ObjectPluginMetrics s3ObjectPluginMetrics,
                         final PluginFactory pluginFactory,
                         final S3ClientBuilderFactory s3ClientBuilderFactory) {
        this.s3ScanBucketOptions = s3SourceConfig.getS3ScanScanOptions().getBuckets();
        this.buffer = buffer;
        this.numberOfRecordsToAccumulate = s3SourceConfig.getNumberOfRecordsToAccumulate();
        this.bufferTimeout = s3SourceConfig.getBufferTimeout();
        this.bucketOwnerProvider = bucketOwnerProvider;
        this.eventConsumer = eventConsumer;
        this.s3ObjectPluginMetrics = s3ObjectPluginMetrics;
        this.pluginFactory = pluginFactory;
        this.s3ClientBuilderFactory = s3ClientBuilderFactory;
        this.endTime = s3SourceConfig.getS3ScanScanOptions().getStartTime();
        this.range = s3SourceConfig.getS3ScanScanOptions().getRange();
        this.compressionOption = s3SourceConfig.getCompression();
    }

    public void start() {
        S3ObjectRequest s3ObjectRequest = new S3ObjectRequest.Builder(buffer, numberOfRecordsToAccumulate, bufferTimeout, s3ObjectPluginMetrics)
                .bucketOwnerProvider(bucketOwnerProvider)
                .s3AsyncClient(s3ClientBuilderFactory.getS3AsyncClient()).s3Client(s3ClientBuilderFactory.getS3Client())
                .eventConsumer(eventConsumer).build();
        Thread t = new Thread(new ScanObjectWorker(s3ObjectRequest, getScanOptions()));
        t.start();
    }

    /**
     * This Method Used to fetch the scan options details from {@link S3SourceConfig} amd build the
     * all the s3 scan buckets information in list.
     *
     * @return @List<ScanOptionsBuilder>
     */
    List<ScanOptionsBuilder> getScanOptions() {
        List<ScanOptionsBuilder> scanOptionsList = new ArrayList<>();
        s3ScanBucketOptions.forEach(
                obj -> {
                    final S3ScanBucketOption bucket = obj.getBucket();
                    final S3SelectOptions s3SelectOptions = bucket.getS3SelectOptions();
                    Codec codec = null;
                    String queryStatement = null;
                    FileHeaderInfo csvFileHeaderInfo = FileHeaderInfo.NONE;
                    S3SelectSerializationFormatOption serializationFormatOption = null;

                    if (s3SelectOptions == null && bucket.getCodec() == null) {
                        throw new NoSuchElementException("codec is required in pipeline yaml configuration");
                    }
                    if (bucket.getCodec() != null) {
                        final PluginModel pluginModel = bucket.getCodec();
                        final PluginSetting pluginSetting = new PluginSetting(pluginModel.getPluginName(),
                                pluginModel.getPluginSettings());
                        codec = pluginFactory.loadPlugin(Codec.class, pluginSetting);
                    }
                    if (s3SelectOptions != null) {
                        queryStatement = s3SelectOptions.getQueryStatement();
                        serializationFormatOption = s3SelectOptions.getS3SelectSerializationFormatOption();
                        csvFileHeaderInfo = FileHeaderInfo.valueOf(s3SelectOptions.getCsvFileHeaderInfo());
                    }
                    scanOptionsList.add(new ScanOptionsBuilder().setStartDate(endTime).setRange(range)
                            .setBucket(bucket.getName()).setQuery(queryStatement)
                            .setSerializationFormatOption(serializationFormatOption)
                            .setKeys(bucket.getKeyPath()).setCodec(codec).setCsvHeaderInfo(csvFileHeaderInfo)
                            .setCompressionOption(bucket.getCompression() != null ? bucket.getCompression() : compressionOption));
                });
        return scanOptionsList;
    }
}
