/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.noop.NoopTimer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.source.s3.configuration.S3DataSelection;
import org.opensearch.dataprepper.plugins.source.s3.ownership.BucketOwnerProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class S3DataSelectionIT {
    private static final int TIMEOUT_IN_MILLIS = 200;
    
    private S3Client s3Client;
    private S3ObjectGenerator s3ObjectGenerator;
    private String bucket;
    private Buffer<Record<Event>> buffer;
    private S3ObjectPluginMetrics s3ObjectPluginMetrics;
    private BucketOwnerProvider bucketOwnerProvider;
    private EventMetadataModifier eventMetadataModifier;

    @BeforeEach
    void setUp() {
        s3Client = S3Client.builder()
                .region(Region.of(System.getProperty("tests.s3source.region")))
                .build();
        bucket = System.getProperty("tests.s3source.bucket");
        s3ObjectGenerator = new S3ObjectGenerator(s3Client, bucket);
        buffer = mock(Buffer.class);
        
        eventMetadataModifier = new EventMetadataModifier(S3SourceConfig.DEFAULT_METADATA_ROOT_KEY, false);
        bucketOwnerProvider = b -> Optional.empty();
        
        s3ObjectPluginMetrics = mock(S3ObjectPluginMetrics.class);
        final Counter counter = mock(Counter.class);
        final DistributionSummary distributionSummary = mock(DistributionSummary.class);
        final Timer timer = new NoopTimer(new Meter.Id("test", Tags.empty(), null, null, Meter.Type.TIMER));
        when(s3ObjectPluginMetrics.getS3ObjectsFailedCounter()).thenReturn(counter);
        when(s3ObjectPluginMetrics.getS3ObjectsSucceededCounter()).thenReturn(counter);
        when(s3ObjectPluginMetrics.getS3ObjectsFailedAccessDeniedCounter()).thenReturn(counter);
        when(s3ObjectPluginMetrics.getS3ObjectsFailedNotFoundCounter()).thenReturn(counter);
        when(s3ObjectPluginMetrics.getS3ObjectSizeSummary()).thenReturn(distributionSummary);
        when(s3ObjectPluginMetrics.getS3ObjectEventsSummary()).thenReturn(distributionSummary);
        when(s3ObjectPluginMetrics.getS3ObjectSizeProcessedSummary()).thenReturn(distributionSummary);
        when(s3ObjectPluginMetrics.getS3ObjectReadTimer()).thenReturn(timer);
        when(s3ObjectPluginMetrics.getS3ObjectNoRecordsFound()).thenReturn(counter);
    }

    @Test
    void data_and_metadata_selection_includes_both_data_and_metadata() throws Exception {
        testS3DataSelection(S3DataSelection.DATA_AND_METADATA, event -> {
            assertThat(event.toMap().containsKey("message"), equalTo(true)); // data content
            assertThat(event.get("s3/bucket", String.class), equalTo(bucket)); // metadata present
        });
    }

    @Test
    void data_only_selection_includes_only_data() throws Exception {
        testS3DataSelection(S3DataSelection.DATA_ONLY, event -> {
            assertThat(event.toMap().containsKey("message"), equalTo(true)); // data content
            assertThat(event.get("s3/bucket", String.class), nullValue()); // no metadata
            assertThat(event.get("s3/key", String.class), nullValue()); // no metadata
        });
    }

    @Test
    void metadata_only_selection_includes_only_metadata() throws Exception {
        testS3DataSelection(S3DataSelection.METADATA_ONLY, event -> {
            assertThat(event.toMap().containsKey("message"), equalTo(false)); // no data content
            assertThat(event.get("bucket", String.class), equalTo(bucket)); // metadata present as top-level key
        });
    }

    private void testS3DataSelection(S3DataSelection dataSelection, Consumer<Event> eventAssertions) throws Exception {
        String key = writeTestObject();
        S3ObjectReference s3ObjectReference = S3ObjectReference.bucketAndKey(bucket, key).build();
        
        stubBufferWriter(eventAssertions);
        
        S3ObjectWorker objectWorker = createObjectUnderTest();
        objectWorker.processS3Object(s3ObjectReference, dataSelection, null, null, null);
        
        verify(buffer).writeAll(anyCollection(), anyInt());
    }

    private void stubBufferWriter(final Consumer<Event> eventAssertions) throws Exception {
        doAnswer(a -> {
            final Collection<Record<Event>> recordsCollection = a.getArgument(0);
            assertThat(recordsCollection.size(), greaterThanOrEqualTo(1));
            for (Record<Event> eventRecord : recordsCollection) {
                assertThat(eventRecord, notNullValue());
                assertThat(eventRecord.getData(), notNullValue());
                eventAssertions.accept(eventRecord.getData());
            }
            return null;
        }).when(buffer).writeAll(anyCollection(), anyInt());
    }

    private S3ObjectWorker createObjectUnderTest() {
        final S3ObjectRequest request = new S3ObjectRequest.Builder(buffer, 100,
                Duration.ofMillis(TIMEOUT_IN_MILLIS), s3ObjectPluginMetrics)
                .bucketOwnerProvider(bucketOwnerProvider)
                .eventConsumer(eventMetadataModifier)
                .codec(new NewlineDelimitedRecordsGenerator().getCodec())
                .s3Client(s3Client)
                .compressionOption(CompressionOption.NONE)
                .build();
        return new S3ObjectWorker(request);
    }

    private String writeTestObject() throws IOException {
        final NewlineDelimitedRecordsGenerator generator = new NewlineDelimitedRecordsGenerator();
        final String key = "s3 source/data-selection/" + UUID.randomUUID() + "_" + Instant.now().toString() + generator.getFileExtension();
        s3ObjectGenerator.write(5, key, generator, false);
        return key;
    }
}
