/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.plugins.source.configuration.S3ScanBucketOption;
import org.opensearch.dataprepper.plugins.source.configuration.S3ScanBucketOptions;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Class responsible for taking an {@link S3SourceConfig} and creating all the necessary {@link ScanOptions}
 * objects and spawn a thread {@link S3SelectObjectWorker}
 */
public class S3ScanService {
    private final List<S3ScanBucketOptions> s3ScanBucketOptions;
    private final S3ClientBuilderFactory s3ClientBuilderFactory;
    private final LocalDateTime endDateTime;

    private final LocalDateTime startDateTime;
    private final Duration range;
    private final S3ObjectHandler s3ObjectHandler;

    private Thread scanObjectWorkerThread;

    private final BucketOwnerProvider bucketOwnerProvider;

    public S3ScanService(final S3SourceConfig s3SourceConfig,
                         final S3ClientBuilderFactory s3ClientBuilderFactory,
                         final S3ObjectHandler s3ObjectHandler,
                         final BucketOwnerProvider bucketOwnerProvider ) {
        this.s3ScanBucketOptions = s3SourceConfig.getS3ScanScanOptions().getBuckets();
        this.s3ClientBuilderFactory = s3ClientBuilderFactory;
        this.endDateTime = s3SourceConfig.getS3ScanScanOptions().getEndTime();
        this.startDateTime = s3SourceConfig.getS3ScanScanOptions().getStartTime();
        this.range = s3SourceConfig.getS3ScanScanOptions().getRange();
        this.s3ObjectHandler = s3ObjectHandler;
        this.bucketOwnerProvider = bucketOwnerProvider;
    }

    public void start() {
        scanObjectWorkerThread = new Thread(new ScanObjectWorker(s3ClientBuilderFactory.getS3Client(),
                getScanOptions(),s3ObjectHandler,bucketOwnerProvider));
        scanObjectWorkerThread.start();
    }

    /**
     * This Method Used to fetch the scan options details from {@link S3SourceConfig} amd build the
     * all the s3 scan buckets information in list.
     *
     * @return @List<ScanOptionsBuilder>
     */
    List<ScanOptions> getScanOptions() {
        List<ScanOptions> scanOptionsList = new ArrayList<>();
        s3ScanBucketOptions.forEach(
                obj -> buildScanOptions(scanOptionsList, obj));
        return scanOptionsList;
    }

    private void buildScanOptions(final List<ScanOptions> scanOptionsList, final S3ScanBucketOptions scanBucketOptions) {
        final S3ScanBucketOption s3ScanBucketOption = scanBucketOptions.getS3ScanBucketOption();
        scanOptionsList.add(new ScanOptions.Builder()
                .setStartDateTime(startDateTime)
                .setEndDateTime(endDateTime)
                .setRange(range)
                .setBucket(s3ScanBucketOption.getName())
                .setS3ScanKeyPathOption(s3ScanBucketOption.getkeyPrefix()).build());
    }
}
