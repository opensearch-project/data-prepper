/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.model.source.coordinator.PartitionIdentifier;
import org.opensearch.dataprepper.plugins.source.configuration.S3ScanKeyPathOption;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.utils.Pair;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class S3ScanPartitionCreationSupplier implements Supplier<List<PartitionIdentifier>> {

    private static final String BUCKET_OBJECT_PARTITION_KEY_FORMAT = "%s|%s";

    private final S3Client s3Client;
    private final BucketOwnerProvider bucketOwnerProvider;
    private final List<ScanOptions> scanOptionsList;
    public S3ScanPartitionCreationSupplier(final S3Client s3Client,
                                           final BucketOwnerProvider bucketOwnerProvider,
                                           final List<ScanOptions> scanOptionsList) {

        this.s3Client = s3Client;
        this.bucketOwnerProvider = bucketOwnerProvider;
        this.scanOptionsList = scanOptionsList;
    }

    @Override
    public List<PartitionIdentifier> get() {
        final List<PartitionIdentifier> objectsToProcess = new ArrayList<>();

        for (final ScanOptions scanOptions : scanOptionsList) {
            final List<String> excludeItems = new ArrayList<>();
            final S3ScanKeyPathOption s3ScanKeyPathOption = scanOptions.getS3ScanKeyPathOption();
            final ListObjectsV2Request.Builder listObjectsV2Request = ListObjectsV2Request.builder()
                    .bucket(scanOptions.getBucket());
            bucketOwnerProvider.getBucketOwner(scanOptions.getBucket())
                    .ifPresent(listObjectsV2Request::expectedBucketOwner);

            if (Objects.nonNull(s3ScanKeyPathOption) && Objects.nonNull(s3ScanKeyPathOption.getS3ScanExcludeSuffixOptions()))
                excludeItems.addAll(s3ScanKeyPathOption.getS3ScanExcludeSuffixOptions());

            if (Objects.nonNull(s3ScanKeyPathOption) && Objects.nonNull(s3ScanKeyPathOption.getS3scanIncludeOptions()))
                s3ScanKeyPathOption.getS3scanIncludeOptions().forEach(includePath -> {
                    listObjectsV2Request.prefix(includePath);
                    objectsToProcess.addAll(listFilteredS3ObjectsForBucket(excludeItems, listObjectsV2Request, scanOptions.getBucket(),
                            scanOptions.getUseStartDateTime(), scanOptions.getUseEndDateTime()));
                });
            else
                objectsToProcess.addAll(listFilteredS3ObjectsForBucket(excludeItems, listObjectsV2Request, scanOptions.getBucket(),
                        scanOptions.getUseStartDateTime(), scanOptions.getUseEndDateTime()));
        }

        return objectsToProcess;
    }

    private List<PartitionIdentifier> listFilteredS3ObjectsForBucket(final List<String> excludeKeyPaths,
                                                    final ListObjectsV2Request.Builder listObjectsV2Request,
                                                    final String bucket,
                                                    final LocalDateTime startDateTime,
                                                    final LocalDateTime endDateTime) {

        final List<PartitionIdentifier> allPartitionIdentifiers = new ArrayList<>();
        ListObjectsV2Response listObjectsV2Response = null;
        do {
            listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request.fetchOwner(true).continuationToken(Objects.nonNull(listObjectsV2Response) ? listObjectsV2Response.nextContinuationToken() : null).build());
            allPartitionIdentifiers.addAll(listObjectsV2Response.contents().stream()
                    .map(s3Object -> Pair.of(s3Object.key(), instantToLocalDateTime(s3Object.lastModified())))
                    .filter(keyTimestampPair -> !keyTimestampPair.left().endsWith("/"))
                    .filter(keyTimestampPair -> excludeKeyPaths.stream()
                            .noneMatch(excludeItem -> keyTimestampPair.left().endsWith(excludeItem)))
                    .filter(keyTimestampPair -> isKeyMatchedBetweenTimeRange(keyTimestampPair.right(), startDateTime, endDateTime))
                    .map(Pair::left)
                    .map(objectKey -> PartitionIdentifier.builder().withPartitionKey(String.format(BUCKET_OBJECT_PARTITION_KEY_FORMAT, bucket, objectKey)).build())
                    .collect(Collectors.toList()));
        } while (listObjectsV2Response.isTruncated());

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
                                                 final LocalDateTime endDateTime){
        return lastModifiedTime.isAfter(startDateTime) && lastModifiedTime.isBefore(endDateTime);
    }
}
