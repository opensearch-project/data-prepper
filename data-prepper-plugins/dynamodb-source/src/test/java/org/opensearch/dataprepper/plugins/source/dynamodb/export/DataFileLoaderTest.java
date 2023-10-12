/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.export;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.converter.ExportRecordConverter;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.DataFilePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.DataFileProgressState;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@Disabled
class DataFileLoaderTest {

    @Mock
    private EnhancedSourceCoordinator coordinator;

    @Mock
    private S3Client s3Client;


    private S3ObjectReader s3ObjectReader;

    @Mock
    private ExportRecordConverter recordConverter;

    private DataFileCheckpointer checkpointer;

    private DataFilePartition dataFilePartition;

    private final String tableName = UUID.randomUUID().toString();
    private final String tableArn = "arn:aws:dynamodb:us-west-2:123456789012:table/" + tableName;

    private final String manifestKey = UUID.randomUUID().toString();
    private final String bucketName = UUID.randomUUID().toString();
    private final String prefix = UUID.randomUUID().toString();

    private final String exportArn = tableArn + "/export/01693291918297-bfeccbea";

    private final Random random = new Random();

    private final int total = random.nextInt(10);

    @BeforeEach
    void setup() throws IOException {

        DataFileProgressState state = new DataFileProgressState();
        state.setLoaded(0);
        state.setTotal(total);

        dataFilePartition = new DataFilePartition(exportArn, bucketName, manifestKey, Optional.of(state));

        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(generateGzipInputStream(total));
        s3ObjectReader = new S3ObjectReader(s3Client);

        lenient().when(coordinator.createPartition(any(EnhancedSourcePartition.class))).thenReturn(true);
        lenient().doNothing().when(coordinator).completePartition(any(EnhancedSourcePartition.class));
        lenient().doNothing().when(coordinator).saveProgressStateForPartition(any(EnhancedSourcePartition.class));
        lenient().doNothing().when(coordinator).giveUpPartition(any(EnhancedSourcePartition.class));

        lenient().doNothing().when(recordConverter).writeToBuffer(any(List.class));

        checkpointer = new DataFileCheckpointer(coordinator, dataFilePartition);

    }

    private ResponseInputStream<GetObjectResponse> generateGzipInputStream(int numberOfRecords) throws IOException {

        StringJoiner stringJoiner = new StringJoiner("\\n");
        for (int i = 0; i < numberOfRecords; i++) {
            stringJoiner.add(UUID.randomUUID().toString());
        }
        final String data = stringJoiner.toString();

        final byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);

        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        final GZIPOutputStream gzipOut = new GZIPOutputStream(byteOut);
        gzipOut.write(dataBytes, 0, dataBytes.length);
        gzipOut.close();
        final byte[] bites = byteOut.toByteArray();
        final ByteArrayInputStream byteInStream = new ByteArrayInputStream(bites);

        final ResponseInputStream<GetObjectResponse> fileInputStream = new ResponseInputStream<>(
                GetObjectResponse.builder().contentLength((long) data.length()).build(),
                AbortableInputStream.create(byteInStream)
        );
        return fileInputStream;

    }

    @Test
    void test_run_loadFile_correctly() throws InterruptedException {

        DataFileLoader loader = DataFileLoader.builder()
                .bucketName(bucketName)
                .key(manifestKey)
                .s3ObjectReader(s3ObjectReader)
                .recordConverter(recordConverter)
                .checkpointer(checkpointer)
                .build();

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        final Future<?> future = executorService.submit(loader);
        Thread.sleep(100);
        executorService.shutdown();
        future.cancel(true);
        assertThat(executorService.awaitTermination(1000, TimeUnit.MILLISECONDS), equalTo(true));

        // Should call s3 getObject
        verify(s3Client).getObject(any(GetObjectRequest.class));

        // Should write to buffer
        verify(recordConverter).writeToBuffer(any(List.class));

        // Should do one last checkpoint when done.
        verify(coordinator).saveProgressStateForPartition(any(DataFilePartition.class));

    }

}