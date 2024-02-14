/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import org.opensearch.dataprepper.model.source.coordinator.PartitionIdentifier;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanKeyPathOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanSchedulingOptions;
import org.opensearch.dataprepper.plugins.source.s3.ownership.BucketOwnerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.utils.Pair;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class S3ScanPartitionCreationSupplier implements Function<Map<String, Object>, List<PartitionIdentifier>> {

    private static final Logger LOG = LoggerFactory.getLogger(S3ScanPartitionCreationSupplier.class);

    private static final String BUCKET_OBJECT_PARTITION_KEY_FORMAT = "%s|%s";
    static final String SCAN_COUNT = "SCAN_COUNT";
    static final String LAST_SCAN_TIME = "LAST_SCAN_TIME";
    static final String SINGLE_SCAN_COMPLETE = "SINGLE_SCAN_COMPLETE";

    private final S3Client s3Client;
    private final BucketOwnerProvider bucketOwnerProvider;
    private final List<ScanOptions> scanOptionsList;
    private final S3ScanSchedulingOptions schedulingOptions;
    public S3ScanPartitionCreationSupplier(final S3Client s3Client,
                                           final BucketOwnerProvider bucketOwnerProvider,
                                           final List<ScanOptions> scanOptionsList,
                                           final S3ScanSchedulingOptions schedulingOptions) {

        this.s3Client = s3Client;
        this.bucketOwnerProvider = bucketOwnerProvider;
        this.scanOptionsList = scanOptionsList;
        this.schedulingOptions = schedulingOptions;
    }

    @Override
    public List<PartitionIdentifier> apply(final Map<String, Object> globalStateMap) {

        if (globalStateMap.isEmpty()) {
          initializeGlobalStateMap(globalStateMap);
        }

        if (shouldScanBeSkipped(globalStateMap)) {
            return Collections.emptyList();
        }

        final List<PartitionIdentifier> objectsToProcess = new ArrayList<>();

        for (final ScanOptions scanOptions : scanOptionsList) {
            final List<String> excludeItems = new ArrayList<>();
            final S3ScanKeyPathOption s3ScanKeyPathOption = scanOptions.getBucketOption().getS3ScanFilter();
            final ListObjectsV2Request.Builder listObjectsV2Request = ListObjectsV2Request.builder()
                    .bucket(scanOptions.getBucketOption().getName());
            bucketOwnerProvider.getBucketOwner(scanOptions.getBucketOption().getName())
                    .ifPresent(listObjectsV2Request::expectedBucketOwner);

            if (Objects.nonNull(s3ScanKeyPathOption) && Objects.nonNull(s3ScanKeyPathOption.getS3ScanExcludeSuffixOptions()))
                excludeItems.addAll(s3ScanKeyPathOption.getS3ScanExcludeSuffixOptions());

            if (Objects.nonNull(s3ScanKeyPathOption) && Objects.nonNull(s3ScanKeyPathOption.getS3scanIncludePrefixOptions()))
                s3ScanKeyPathOption.getS3scanIncludePrefixOptions().forEach(includePath -> {
                    listObjectsV2Request.prefix(includePath);
                    objectsToProcess.addAll(listFilteredS3ObjectsForBucket(excludeItems, listObjectsV2Request,
                            scanOptions.getBucketOption().getName(), scanOptions.getUseStartDateTime(), scanOptions.getUseEndDateTime(), globalStateMap));
                });
            else
                objectsToProcess.addAll(listFilteredS3ObjectsForBucket(excludeItems, listObjectsV2Request,
                        scanOptions.getBucketOption().getName(), scanOptions.getUseStartDateTime(), scanOptions.getUseEndDateTime(), globalStateMap));
        }

        globalStateMap.put(SCAN_COUNT, (Integer) globalStateMap.get(SCAN_COUNT) + 1);
        globalStateMap.put(LAST_SCAN_TIME, Instant.now().toEpochMilli());

        return objectsToProcess;
    }

    private List<PartitionIdentifier> listFilteredS3ObjectsForBucket(final List<String> excludeKeyPaths,
                                                                     final ListObjectsV2Request.Builder listObjectsV2Request,
                                                                     final String bucket,
                                                                     final LocalDateTime startDateTime,
                                                                     final LocalDateTime endDateTime,
                                                                     final Map<String, Object> globalStateMap) {
        Instant mostRecentLastModifiedTimestamp = globalStateMap.get(bucket) != null ? Instant.parse((String) globalStateMap.get(bucket)) : null;
        final List<PartitionIdentifier> allPartitionIdentifiers = new ArrayList<>();
        ListObjectsV2Response listObjectsV2Response = null;
        do {
            listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request.fetchOwner(true).continuationToken(Objects.nonNull(listObjectsV2Response) ? listObjectsV2Response.nextContinuationToken() : null).build());
            allPartitionIdentifiers.addAll(listObjectsV2Response.contents().stream()
                    .filter(s3Object -> isLastModifiedTimeAfterMostRecentScanForBucket(bucket, s3Object, globalStateMap))
                    .map(s3Object -> Pair.of(s3Object.key(), instantToLocalDateTime(s3Object.lastModified())))
                    .filter(keyTimestampPair -> !keyTimestampPair.left().endsWith("/"))
                    .filter(keyTimestampPair -> excludeKeyPaths.stream()
                            .noneMatch(excludeItem -> keyTimestampPair.left().endsWith(excludeItem)))
                    .filter(keyTimestampPair -> isKeyMatchedBetweenTimeRange(keyTimestampPair.right(), startDateTime, endDateTime))
                    .map(Pair::left)
                    .map(objectKey -> PartitionIdentifier.builder().withPartitionKey(String.format(BUCKET_OBJECT_PARTITION_KEY_FORMAT, bucket, objectKey)).build())
                    .collect(Collectors.toList()));

            LOG.info("Found page of {} objects from bucket {}", listObjectsV2Response.keyCount(), bucket);

            mostRecentLastModifiedTimestamp = getMostRecentLastModifiedTimestamp(listObjectsV2Response, mostRecentLastModifiedTimestamp);
        } while (listObjectsV2Response.isTruncated());

        globalStateMap.put(bucket, Objects.nonNull(mostRecentLastModifiedTimestamp) ? mostRecentLastModifiedTimestamp.toString() : null);

        LOG.info("Returning partitions for {} S3 objects from bucket {}", allPartitionIdentifiers.size(), bucket);
        return allPartitionIdentifiers;
    }

    private LocalDateTime instantToLocalDateTime(final Instant instant) {
        final ZonedDateTime zonedDateTime = instant.atZone(ZoneId.systemDefault());
        return zonedDateTime.toLocalDateTime();
    }

    /**
     * Used for identifying s3 object last modified time match with scan the date range.
     * @return boolean
     */
    private boolean isKeyMatchedBetweenTimeRange(final LocalDateTime lastModifiedTime,
                                                 final LocalDateTime startDateTime,
                                                 final LocalDateTime endDateTime) {
        if (Objects.nonNull(schedulingOptions)) {
            return true;
        } else if (Objects.isNull(startDateTime) && Objects.isNull(endDateTime)) {
            return true;
        } else if (Objects.isNull(startDateTime)) {
            return lastModifiedTime.isBefore(endDateTime);
        } else if (Objects.isNull(endDateTime)) {
            return lastModifiedTime.isAfter(startDateTime);
        }
        return lastModifiedTime.isAfter(startDateTime) && lastModifiedTime.isBefore(endDateTime);
    }

    private void initializeGlobalStateMap(final Map<String, Object> globalStateMap) {
        globalStateMap.put(SCAN_COUNT, 0);
        globalStateMap.put(SINGLE_SCAN_COMPLETE, false);
    }

    private boolean isLastModifiedTimeAfterMostRecentScanForBucket(final String bucketName,
                                                                   final S3Object s3Object,
                                                                   final Map<String, Object> globalStateMap) {
        if (!globalStateMap.containsKey(bucketName) || Objects.isNull(globalStateMap.get(bucketName))) {
            return true;
        }

        final Instant lastProcessedObjectTimestamp = Instant.parse((String) globalStateMap.get(bucketName));

        return s3Object.lastModified().compareTo(lastProcessedObjectTimestamp.minusSeconds(1)) >= 0;
    }

    private Instant getMostRecentLastModifiedTimestamp(final ListObjectsV2Response listObjectsV2Response,
                                                       Instant mostRecentLastModifiedTimestamp) {

        if (Objects.isNull(schedulingOptions)) {
            return null;
        }

        for (final S3Object s3Object : listObjectsV2Response.contents()) {
            if (Objects.isNull(mostRecentLastModifiedTimestamp) || s3Object.lastModified().isAfter(mostRecentLastModifiedTimestamp)) {
                mostRecentLastModifiedTimestamp = s3Object.lastModified();
            }
        }

        return mostRecentLastModifiedTimestamp;
    }

    private boolean shouldScanBeSkipped(final Map<String, Object> globalStateMap) {

        if (Objects.isNull(schedulingOptions) && hasAlreadyBeenScanned(globalStateMap)) {

            if (!(Boolean) globalStateMap.get(SINGLE_SCAN_COMPLETE)) {
                LOG.info("Single S3 scan has already been completed");
                globalStateMap.put(SINGLE_SCAN_COMPLETE, true);
            }

            return true;
        }

        if (Objects.nonNull(schedulingOptions) &&
                (hasReachedMaxScanCount(globalStateMap) || !hasReachedScheduledScanTime(globalStateMap))) {



            if (hasReachedMaxScanCount(globalStateMap)) {
                LOG.info("Skipping scan as the max scan count {} has been reached", schedulingOptions.getCount());
            } else {
                LOG.info("Skipping scan as the interval of {} seconds has not been reached yet", schedulingOptions.getInterval().toSeconds());
            }

            return true;
        }

        return false;
    }

    private boolean hasAlreadyBeenScanned(final Map<String, Object> globalStateMap) {
        return (Integer) globalStateMap.get(SCAN_COUNT) > 0;
    }

    private boolean hasReachedMaxScanCount(final Map<String, Object> globalStateMap) {
        return (Integer) globalStateMap.get(SCAN_COUNT) >= schedulingOptions.getCount();
    }

    private boolean hasReachedScheduledScanTime(final Map<String, Object> globalStateMap) {
        if (!globalStateMap.containsKey(LAST_SCAN_TIME)) {
            return true;
        }

        return Instant.now().minus(schedulingOptions.getInterval()).isAfter(Instant.ofEpochMilli((Long) globalStateMap.get(LAST_SCAN_TIME)));
    }
}
