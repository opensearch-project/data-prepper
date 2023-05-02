/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.plugins.source.configuration.S3ScanKeyPathOption;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Class responsible for processing the s3 scan objects with the help of <code>S3ObjectWorker</code>
 * or <code>S3SelectWorker</code>.
 */
public class ScanObjectWorker implements Runnable{

    private static final Logger LOG = LoggerFactory.getLogger(ScanObjectWorker.class);

    private final S3Client s3Client;

    private static final Map<String,S3ObjectDetails> stateSaveMap = new HashMap<>();

    private final List<ScanOptions> scanOptionsBuilderList;

    private final S3ObjectHandler s3ObjectHandler;

    private final BucketOwnerProvider bucketOwnerProvider;

    public ScanObjectWorker(final S3Client s3Client,
                            final List<ScanOptions> scanOptionsBuilderList,
                            final S3ObjectHandler s3ObjectHandler,
                            final BucketOwnerProvider bucketOwnerProvider){
        this.s3Client = s3Client;
        this.scanOptionsBuilderList = scanOptionsBuilderList;
        this.s3ObjectHandler= s3ObjectHandler;
        this.bucketOwnerProvider = bucketOwnerProvider;
    }

    /**
     * It will decide the s3 object parse <code>S3ObjectWorker</code> or <code>S3SelectWorker</code>
     * based on s3 select configuration provided.
     */
    @Override
    public void run() {
        scanOptionsBuilderList.forEach(this::parseS3ScanObjects);
    }

    /**
     * Method will parse the s3 object and write to {@link Buffer}
     */
    void parseS3ScanObjects(final ScanOptions scanOptions) {
        final List<String> scanObjects = new ArrayList<>();
        final List<String> excludeItems = new ArrayList<>();
        final S3ScanKeyPathOption s3ScanKeyPathOption = scanOptions.getS3ScanKeyPathOption();
        final ListObjectsV2Request.Builder listObjectsV2Request = ListObjectsV2Request.builder()
                .bucket(scanOptions.getBucket());
        bucketOwnerProvider.getBucketOwner(scanOptions.getBucket())
                .ifPresent(listObjectsV2Request::expectedBucketOwner);

        if(Objects.nonNull(s3ScanKeyPathOption) && Objects.nonNull(s3ScanKeyPathOption.getS3ScanExcludeSuffixOptions()))
            excludeItems.addAll(s3ScanKeyPathOption.getS3ScanExcludeSuffixOptions());

        if(Objects.nonNull(s3ScanKeyPathOption) && Objects.nonNull(s3ScanKeyPathOption.getS3scanIncludeOptions()))
            s3ScanKeyPathOption.getS3scanIncludeOptions().forEach(includePath -> {
                listObjectsV2Request.prefix(includePath);
                scanObjects.addAll(listS3Objects(excludeItems, listObjectsV2Request));
            });
        else
            scanObjects.addAll(listS3Objects(excludeItems, listObjectsV2Request));

        if(scanObjects.isEmpty())
            LOG.info("s3 objects are not found in configured scan pipeline.");

        scanObjects.forEach(key ->
                processS3ObjectKeys(S3ObjectReference.bucketAndKey(scanOptions.getBucket(),
                        key).build(),s3ObjectHandler, scanOptions));
    }

    private List<String> listS3Objects(List<String> excludeKeyPaths, ListObjectsV2Request.Builder listObjectsV2Request) {
          return s3Client.listObjectsV2(listObjectsV2Request.fetchOwner(true).build()).contents().stream().map(S3Object::key)
                .filter(path -> !path.endsWith("/"))
                .filter(includeKeyPath -> excludeKeyPaths.stream()
                        .noneMatch(excludeItem -> includeKeyPath.endsWith(excludeItem))).collect(Collectors.toList());
    }

    private void processS3ObjectKeys(final S3ObjectReference s3ObjectReference,
                                     final S3ObjectHandler s3ObjectHandler,
                                     final ScanOptions scanOptions){
        final S3ObjectDetails s3ObjDetails = getS3ObjectDetails(s3ObjectReference);
        final boolean isKeyMatchedBetweenTimeRange = isKeyMatchedBetweenTimeRange(s3ObjDetails.getS3ObjectLastModifiedTimestamp(),
                scanOptions.getUseStartDateTime(),
                scanOptions.getUseEndDateTime());
        if(isKeyMatchedBetweenTimeRange && (isKeyNotProcessedByS3Scan(s3ObjDetails))){
            updateKeyProcessedByS3Scan(s3ObjDetails);
            try{
                s3ObjectHandler.parseS3Object(s3ObjectReference,null);
            }catch (IOException ex){
                deleteKeyProcessedByS3Scan(s3ObjDetails);
                LOG.error("Error while process the parseS3Object. ",ex);
            }
        }
    }
    /**
     * Method will identify already processed key.
     * @return boolean
     */
    private boolean isKeyNotProcessedByS3Scan(final S3ObjectDetails s3ObjectDetails) {
        return stateSaveMap.get(s3ObjectDetails.getBucket()+s3ObjectDetails.getKey()) == null;
    }

    /**
     * store the processed bucket key in the map.
     */
    private void updateKeyProcessedByS3Scan(final S3ObjectDetails s3ObjectDetails) {
        stateSaveMap.put((s3ObjectDetails.getBucket() + s3ObjectDetails.getKey()),s3ObjectDetails);
    }
    private void deleteKeyProcessedByS3Scan(S3ObjectDetails s3ObjDetails) {
        stateSaveMap.remove(s3ObjDetails.getBucket() + s3ObjDetails.getKey());
    }

    /**
     * fetch the s3 object last modified time.
     * @return S3ObjectDetails
     */
    private S3ObjectDetails getS3ObjectDetails(final S3ObjectReference s3ObjectReference){
        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(s3ObjectReference.getBucketName()).key(s3ObjectReference.getKey()).build();
        ResponseInputStream<GetObjectResponse> s3ObjectResp = s3Client.getObject(getObjectRequest);
        final Instant instant = s3ObjectResp.response().lastModified();
        ZonedDateTime zonedDateTime = instant.atZone(ZoneId.systemDefault());
        return new S3ObjectDetails(s3ObjectReference.getBucketName(),s3ObjectReference.getKey(),zonedDateTime.toLocalDateTime());
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
