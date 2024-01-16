/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.export;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.dynamodb.converter.ExportRecordConverter;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.DataFilePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.DataFileProgressState;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableInfo;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableMetadata;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.dynamodb.export.DataFileLoader.BUFFER_TIMEOUT;
import static org.opensearch.dataprepper.plugins.source.dynamodb.export.DataFileLoader.DEFAULT_BUFFER_BATCH_SIZE;

@ExtendWith(MockitoExtension.class)
class DataFileLoaderTest {

    @Mock
    private S3Client s3Client;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private BufferAccumulator<Record<Event>> bufferAccumulator;

    @Mock
    private ExportRecordConverter exportRecordConverter;

    @Mock
    private DataFileCheckpointer checkpointer;


    private S3ObjectReader objectReader;

    private DataFilePartition dataFilePartition;

    private TableInfo tableInfo;


    private final String tableName = UUID.randomUUID().toString();
    private final String tableArn = "arn:aws:dynamodb:us-west-2:123456789012:table/" + tableName;

    private final String partitionKeyAttrName = "PK";
    private final String sortKeyAttrName = "SK";

    private final String manifestKey = UUID.randomUUID().toString();
    private final String bucketName = UUID.randomUUID().toString();

    private final String exportArn = tableArn + "/export/01693291918297-bfeccbea";
    private final String exportTime = "1976-01-01T00:00:00Z";

    private final Random random = new Random();

    private final int total = random.nextInt(10) + 1;

    @BeforeEach
    void setup() {

        DataFileProgressState state = new DataFileProgressState();
        state.setLoaded(0);
        state.setTotal(total);
        state.setStartTime(Instant.parse(exportTime).toEpochMilli());

        dataFilePartition = new DataFilePartition(exportArn, bucketName, manifestKey, Optional.of(state));

        TableMetadata metadata = TableMetadata.builder()
                .exportRequired(true)
                .streamRequired(true)
                .partitionKeyAttributeName(partitionKeyAttrName)
                .sortKeyAttributeName(sortKeyAttrName)
                .build();
        tableInfo = new TableInfo(tableArn, metadata);

        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(generateGzipInputStream(total));
        objectReader = new S3ObjectReader(s3Client);
    }

    private ResponseInputStream<GetObjectResponse> generateGzipInputStream(int numberOfRecords) {

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numberOfRecords; i++) {
            final String pk = UUID.randomUUID().toString();
            final String sk = UUID.randomUUID().toString();
            String line = " $ion_1_0 {Item:{PK:\"" + pk + "\",SK:\"" + sk + "\"}}";
            sb.append(line + "\n");
        }
        final String data = sb.toString();

        final byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);

        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        try (final GZIPOutputStream gzipOut = new GZIPOutputStream(byteOut)) {
            gzipOut.write(dataBytes, 0, dataBytes.length);
        } catch (IOException e) {
            e.printStackTrace();
        }

        final byte[] bites = byteOut.toByteArray();
        final ByteArrayInputStream byteInStream = new ByteArrayInputStream(bites);

        final ResponseInputStream<GetObjectResponse> fileInputStream = new ResponseInputStream<>(
                GetObjectResponse.builder().contentLength((long) data.length()).build(),
                AbortableInputStream.create(byteInStream)
        );
        return fileInputStream;

    }

    @Test
    void test_run_loadFile_correctly() {
        DataFileLoader loader;
        try (
                final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class);
                final MockedConstruction<ExportRecordConverter> recordConverterMockedConstruction = mockConstruction(ExportRecordConverter.class, (mock, context) -> {
                    exportRecordConverter = mock;
                })) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT)).thenReturn(bufferAccumulator);
            loader = DataFileLoader.builder(objectReader, pluginMetrics, buffer)
                    .bucketName(bucketName)
                    .key(manifestKey)
                    .checkpointer(checkpointer)
                    .tableInfo(tableInfo)
                    .build();
        }

        loader.run();

        // Should call s3 getObject
        verify(s3Client).getObject(any(GetObjectRequest.class));

        verify(exportRecordConverter).writeToBuffer(eq(null), anyList());

        verify(checkpointer).checkpoint(total);
        verify(checkpointer, never()).updateDatafileForAcknowledgmentWait(any(Duration.class));
    }

    @Test
    void run_loadFile_with_acknowledgments_processes_correctly() {

        final AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        final Duration acknowledgmentTimeout = Duration.ofSeconds(30);

        DataFileLoader loader;
        try (
                final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class);
                final MockedConstruction<ExportRecordConverter> recordConverterMockedConstruction = mockConstruction(ExportRecordConverter.class, (mock, context) -> {
                    exportRecordConverter = mock;
                })) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT)).thenReturn(bufferAccumulator);
            loader = DataFileLoader.builder(objectReader, pluginMetrics, buffer)
                    .bucketName(bucketName)
                    .key(manifestKey)
                    .checkpointer(checkpointer)
                    .tableInfo(tableInfo)
                    .acknowledgmentSet(acknowledgementSet)
                    .acknowledgmentSetTimeout(acknowledgmentTimeout)
                    .build();
        }

        loader.run();

        // Should call s3 getObject
        verify(s3Client).getObject(any(GetObjectRequest.class));

        verify(exportRecordConverter).writeToBuffer(eq(acknowledgementSet), anyList());

        verify(checkpointer).checkpoint(total);
        verify(checkpointer).updateDatafileForAcknowledgmentWait(acknowledgmentTimeout);


        verify(acknowledgementSet).complete();
    }

}