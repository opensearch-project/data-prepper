/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.PartitionIdentifier;
import org.opensearch.dataprepper.plugins.source.configuration.S3ScanKeyPathOption;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
public class S3ScanPartitionCreationSupplierTest {

    @Mock
    private S3Client s3Client;

    @Mock
    private BucketOwnerProvider bucketOwnerProvider;

    private List<ScanOptions> scanOptionsList;

    @BeforeEach
    void setup() {
        scanOptionsList = new ArrayList<>();
    }


    private Supplier<List<PartitionIdentifier>> createObjectUnderTest() {
        return new S3ScanPartitionCreationSupplier(s3Client, bucketOwnerProvider, scanOptionsList);
    }

    @Test
    void getNextPartition_supplier_returns_expected_PartitionIdentifiers() {

        final String firstBucket = UUID.randomUUID().toString();
        final String secondBucket = UUID.randomUUID().toString();

        final Instant startTime = Instant.now();
        final Instant endTime = Instant.now().plus(3, ChronoUnit.MINUTES);


        final ScanOptions firstBucketScanOptions = mock(ScanOptions.class);
        given(firstBucketScanOptions.getBucket()).willReturn(firstBucket);
        given(firstBucketScanOptions.getUseStartDateTime()).willReturn(LocalDateTime.ofInstant(startTime, ZoneId.systemDefault()));
        given(firstBucketScanOptions.getUseEndDateTime()).willReturn(LocalDateTime.ofInstant(endTime, ZoneId.systemDefault()));
        final S3ScanKeyPathOption firstBucketScanKeyPath = mock(S3ScanKeyPathOption.class);
        given(firstBucketScanOptions.getS3ScanKeyPathOption()).willReturn(firstBucketScanKeyPath);
        given(firstBucketScanKeyPath.getS3scanIncludeOptions()).willReturn(List.of(UUID.randomUUID().toString()));
        given(firstBucketScanKeyPath.getS3ScanExcludeSuffixOptions()).willReturn(List.of(".invalid"));
        scanOptionsList.add(firstBucketScanOptions);

        final ScanOptions secondBucketScanOptions = mock(ScanOptions.class);
        given(secondBucketScanOptions.getBucket()).willReturn(secondBucket);
        given(secondBucketScanOptions.getUseStartDateTime()).willReturn(LocalDateTime.ofInstant(startTime, ZoneId.systemDefault()));
        given(secondBucketScanOptions.getUseEndDateTime()).willReturn(LocalDateTime.ofInstant(endTime, ZoneId.systemDefault()));
        final S3ScanKeyPathOption secondBucketScanKeyPath = mock(S3ScanKeyPathOption.class);
        given(secondBucketScanOptions.getS3ScanKeyPathOption()).willReturn(secondBucketScanKeyPath);
        given(secondBucketScanKeyPath.getS3scanIncludeOptions()).willReturn(null);
        given(secondBucketScanKeyPath.getS3ScanExcludeSuffixOptions()).willReturn(null);
        scanOptionsList.add(secondBucketScanOptions);

        final Supplier<List<PartitionIdentifier>> partitionCreationSupplier = createObjectUnderTest();

        final List<PartitionIdentifier> expectedPartitionIdentifiers = new ArrayList<>();

        final ListObjectsV2Response listObjectsResponse = mock(ListObjectsV2Response.class);
        final List<S3Object> s3ObjectsList = new ArrayList<>();

        final S3Object invalidFolderObject = mock(S3Object.class);
        given(invalidFolderObject.key()).willReturn("folder-key/");
        given(invalidFolderObject.lastModified()).willReturn(Instant.now());
        s3ObjectsList.add(invalidFolderObject);

        final S3Object invalidForFirstBucketSuffixObject = mock(S3Object.class);
        given(invalidForFirstBucketSuffixObject.key()).willReturn("test.invalid");
        given(invalidForFirstBucketSuffixObject.lastModified()).willReturn(Instant.now());
        s3ObjectsList.add(invalidForFirstBucketSuffixObject);
        expectedPartitionIdentifiers.add(PartitionIdentifier.builder().withPartitionKey(secondBucket + "|" + invalidForFirstBucketSuffixObject.key()).build());

        final S3Object invalidDueToLastModifiedOutsideOfStartEndObject = mock(S3Object.class);
        given(invalidDueToLastModifiedOutsideOfStartEndObject.key()).willReturn(UUID.randomUUID().toString());
        given(invalidDueToLastModifiedOutsideOfStartEndObject.lastModified()).willReturn(Instant.now().minus(3, ChronoUnit.MINUTES));
        s3ObjectsList.add(invalidDueToLastModifiedOutsideOfStartEndObject);

        final S3Object validObject = mock(S3Object.class);
        given(validObject.key()).willReturn("valid");
        given(validObject.lastModified()).willReturn(Instant.now());
        s3ObjectsList.add(validObject);
        expectedPartitionIdentifiers.add(PartitionIdentifier.builder().withPartitionKey(firstBucket + "|" + validObject.key()).build());
        expectedPartitionIdentifiers.add(PartitionIdentifier.builder().withPartitionKey(secondBucket + "|" + validObject.key()).build());

        given(listObjectsResponse.contents()).willReturn(s3ObjectsList);

        final ArgumentCaptor<ListObjectsV2Request> listObjectsV2RequestArgumentCaptor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        given(s3Client.listObjectsV2(listObjectsV2RequestArgumentCaptor.capture())).willReturn(listObjectsResponse);

        final List<PartitionIdentifier> resultingPartitions = partitionCreationSupplier.get();

        assertThat(resultingPartitions, notNullValue());
        assertThat(resultingPartitions.size(), equalTo(expectedPartitionIdentifiers.size()));
        assertThat(resultingPartitions.stream().map(PartitionIdentifier::getPartitionKey).collect(Collectors.toList()),
                containsInAnyOrder(expectedPartitionIdentifiers.stream().map(PartitionIdentifier::getPartitionKey).map(Matchers::equalTo).collect(Collectors.toList())));
    }
}
