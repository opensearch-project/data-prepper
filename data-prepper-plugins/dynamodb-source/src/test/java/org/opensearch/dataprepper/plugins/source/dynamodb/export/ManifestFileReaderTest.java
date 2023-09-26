/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.export;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.ExportSummary;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ManifestFileReaderTest {


    @Mock
    private S3ObjectReader s3ObjectReader;


    @Test
    void parseSummaryFile() {

        final Random random = new Random();

        final String version = "2020-06-30";
        final String exportArn = UUID.randomUUID().toString();
        final String startTime = "2023-08-14T08:48:17.020Z";
        final String endTime = "2023-08-14T08:53:26.545Z";
        final String tableArn = UUID.randomUUID().toString();
        final String tableId = UUID.randomUUID().toString();
        final String exportTime = "2023-08-14T08:48:16.212Z";
        final String s3Bucket = UUID.randomUUID().toString();
        final String s3Prefix = UUID.randomUUID().toString();
        final String s3SseAlgorithm = "AES256";
        final String manifestFilesS3Key = UUID.randomUUID().toString();
        final String outputFormat = "DYNAMODB_JSON";
        long billedSizeBytes = random.nextLong();
        int itemCount = random.nextInt(10000);


        String summaryData = String.format("{\"version\":\"%s\",\"exportArn\": \"%s\",\"startTime\":\"%s\",\"endTime\":\"%s\",\"tableArn\":\"%s\",\"tableId\":\"%s\",\"exportTime\":\"%s\",\"s3Bucket\":\"%s\",\"s3Prefix\":\"%s\",\"s3SseAlgorithm\":\"%s\",\"s3SseKmsKeyId\":null,\"manifestFilesS3Key\":\"%s\",\"billedSizeBytes\":%d,\"itemCount\":%d,\"outputFormat\":\"%s\"}",
                version, exportArn, startTime, endTime, tableArn, tableId, exportTime, s3Bucket, s3Prefix, s3SseAlgorithm, manifestFilesS3Key, billedSizeBytes, itemCount, outputFormat);
        InputStream fileObjectStream = new ByteArrayInputStream(summaryData.getBytes());

        when(s3ObjectReader.readFile(anyString(), anyString())).thenReturn(fileObjectStream);

        ManifestFileReader reader = new ManifestFileReader(s3ObjectReader);
        ExportSummary exportSummary = reader.parseSummaryFile("test", "test");

        assertThat(exportSummary, notNullValue());
        assertThat(exportSummary.getVersion(), equalTo(version));
        assertThat(exportSummary.getExportArn(), equalTo(exportArn));
        assertThat(exportSummary.getStartTime(), equalTo(startTime));
        assertThat(exportSummary.getEndTime(), equalTo(endTime));
        assertThat(exportSummary.getTableArn(), equalTo(tableArn));
        assertThat(exportSummary.getTableId(), equalTo(tableId));
        assertThat(exportSummary.getExportTime(), equalTo(exportTime));
        assertThat(exportSummary.getS3Bucket(), equalTo(s3Bucket));
        assertThat(exportSummary.getS3Prefix(), equalTo(s3Prefix));
        assertThat(exportSummary.getS3SseAlgorithm(), equalTo(s3SseAlgorithm));
        assertThat(exportSummary.getManifestFilesS3Key(), equalTo(manifestFilesS3Key));
        assertNull(exportSummary.getS3SseKmsKeyId());
        assertThat(exportSummary.getBilledSizeBytes(), equalTo(billedSizeBytes));
        assertThat(exportSummary.getItemCount(), equalTo(itemCount));
        assertThat(exportSummary.getOutputFormat(), equalTo(outputFormat));


    }

    @Test
    void parseDataFile() {

        final String dataFileS3Key1 = UUID.randomUUID().toString();
        final String dataFileS3Key2 = UUID.randomUUID().toString();

        final Random random = new Random();
        final int itemCount1 = random.nextInt(10000);
        final int itemCount2 = random.nextInt(10000);

        String summaryData = "{\"itemCount\":" + itemCount1 + ",\"md5Checksum\":\"a0k21IY3eelgr2PuWJLjJw==\",\"etag\":\"51f9f394903c5d682321c6211aae8b6a-1\",\"dataFileS3Key\":\"" + dataFileS3Key1 + "\"}\n" +
                "{\"itemCount\":" + itemCount2 + ",\"md5Checksum\":\"j76iIYpnqVKrD/zt6HKV1Q==\",\"etag\":\"551fa137e144877aacf87f75340483bf-1\",\"dataFileS3Key\":\"" + dataFileS3Key2 + "\"}";
        InputStream fileObjectStream = new ByteArrayInputStream(summaryData.getBytes());
        when(s3ObjectReader.readFile(anyString(), anyString())).thenReturn(fileObjectStream);

        ManifestFileReader reader = new ManifestFileReader(s3ObjectReader);
        Map<String, Integer> dataFiles = reader.parseDataFile("test", "test");
        System.out.println(dataFiles);

        assertThat(dataFiles, notNullValue());
        assertThat(dataFiles.size(), equalTo(2));
        assertThat(dataFiles.get(dataFileS3Key1), equalTo(itemCount1));
        assertThat(dataFiles.get(dataFileS3Key2), equalTo(itemCount2));

    }
}