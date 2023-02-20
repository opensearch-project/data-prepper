package org.opensearch.dataprepper.plugins.source;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.codec.Codec;
import org.opensearch.dataprepper.plugins.source.compression.CompressionEngine;
import org.opensearch.dataprepper.plugins.source.configuration.S3SelectOptions;
import org.opensearch.dataprepper.plugins.source.ownership.BucketOwnerProvider;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CSVInput;
import software.amazon.awssdk.services.s3.model.JSONInput;
import software.amazon.awssdk.services.s3.model.JSONOutput;
import software.amazon.awssdk.services.s3.model.JSONType;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.SelectObjectContentEventStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;

@ExtendWith(MockitoExtension.class)
class S3SelectObjectWorkerTest {
	@Mock
	private S3Client s3Client;
	@Mock
	private Buffer<Record<Event>> buffer;
	@Mock
	private CompressionEngine compressionEngine;
	@Mock
	private Codec codec;
	@Mock
	private BucketOwnerProvider bucketOwnerProvider;
	@Mock
	private BiConsumer<Event, S3ObjectReference> eventConsumer;
	@Mock
	private S3AsyncClient s3AsyncClient;
	@Mock
	private S3SourceConfig s3SourceConfig;
	@Mock
	private PluginMetrics pluginMetrics;
	@Mock
	private BufferAccumulator<Record<Event>> bufferAccumulator;
	@Mock
	private DistributionSummary s3SelectSizeProcessedSummary;
	@Mock
	private DistributionSummary s3SelectEventsSummary;
	@Mock
	private Counter s3SelectFailedCounter;
	@InjectMocks
	private S3SelectObjectWorker s3SelectObjectWorker;
	@Mock
	S3ObjectReference s3ObjectReference;
	@Mock
	private PluginModel pluginModel;
	@Mock
	private SelectObjectContentRequest selectObjectContentRequest;
	@Mock
	private S3SelectResponseHandler responseHand;
	@Mock
	CompletableFuture<Void> selectObjectContent;
	@Mock
	SelectObjectContentEventStream contentEventStream;
	@Mock
	private ArrayList<SelectObjectContentEventStream> receivedEvents;
	
	@Mock
	private S3SelectOptions s3SelectOptions;
	private String key;
	private String bucketName;

	private S3SelectObjectWorker createSelectObjectUnderTest() {
		return new S3SelectObjectWorker(s3AsyncClient, s3SourceConfig, pluginMetrics);
	}

	private void setupForS3Select(final String format) {
		bucketName = UUID.randomUUID().toString();
		key = UUID.randomUUID().toString();
		when(s3ObjectReference.getBucketName()).thenReturn(bucketName);
		when(s3ObjectReference.getKey()).thenReturn(key);
		when(s3SourceConfig.getS3SelectOptions()).thenReturn(s3SelectOptions);
		when(pluginMetrics.counter("s3SelectObjectsFailed")).thenReturn(s3SelectFailedCounter);
		when(pluginMetrics.summary("s3SelectProcessedBytes")).thenReturn(s3SelectSizeProcessedSummary);
		when(pluginMetrics.summary("s3SelectObjectsEvents")).thenReturn(s3SelectEventsSummary);
		when(s3SourceConfig.getS3SelectOptions().getQueryStatement()).thenReturn("select * from s3Object");
		when(s3SourceConfig.getS3SelectOptions().getDataSerializationFormat()).thenReturn(format);
		s3SelectObjectWorker = createSelectObjectUnderTest();
	}

	@Test
	public void selectObjectFromS3_with_data_serialization_format_empty() throws Exception {
		when(s3SourceConfig.getS3SelectOptions()).thenReturn(s3SelectOptions);
		when(s3SourceConfig.getS3SelectOptions().getDataSerializationFormat()).thenReturn("");
		String errorMessage = "data_serialization_format is mandatory configuration if s3 select query defined in "
				+ "pipeline configuration";

		Exception actual = assertThrows(RuntimeException.class,
				() -> s3SelectObjectWorker.selectObjectFromS3(s3ObjectReference, bufferAccumulator));

		assertThat(errorMessage, equalTo(actual.getMessage()));

	}

	@Test
	public void selectObjectFromS3_csv_test_with_correct_request() throws Exception {
		setupForS3Select("csv");
		final CSVInput csvInput = CSVInput.builder().fileHeaderInfo("USE").build();
		final JSONOutput jsonOut = JSONOutput.builder().build();

		when(s3AsyncClient.selectObjectContent(any(SelectObjectContentRequest.class),
				any(S3SelectResponseHandler.class))).thenReturn(selectObjectContent);

		final ArgumentCaptor<SelectObjectContentRequest> request = ArgumentCaptor
				.forClass(SelectObjectContentRequest.class);
		s3SelectObjectWorker.selectObjectFromS3(s3ObjectReference, bufferAccumulator);
		verify(s3AsyncClient).selectObjectContent(request.capture(), any(S3SelectResponseHandler.class));
		assertThat(request.getValue().key(), equalTo(key));
		assertThat(request.getValue().bucket(), equalTo(bucketName));
		assertThat(request.getValue().expectedBucketOwner(), nullValue());
		assertThat(request.getValue().expression(), equalTo(s3SourceConfig.getS3SelectOptions().getQueryStatement()));
		assertThat(request.getValue().inputSerialization().csv(), equalTo(csvInput));
		assertThat(request.getValue().outputSerialization().json(), equalTo(jsonOut));
	}

	@Test
	public void selectObjectFromS3_json_test_with_correct_request() throws Exception {
		setupForS3Select("json");

		when(s3AsyncClient.selectObjectContent(any(SelectObjectContentRequest.class),
				any(S3SelectResponseHandler.class))).thenReturn(selectObjectContent);

		final JSONInput jsonInput = JSONInput.builder().type(JSONType.DOCUMENT).build();
		final JSONOutput jsonOut = JSONOutput.builder().build();
		final ArgumentCaptor<SelectObjectContentRequest> request = ArgumentCaptor
				.forClass(SelectObjectContentRequest.class);
		s3SelectObjectWorker.selectObjectFromS3(s3ObjectReference, bufferAccumulator);
		verify(s3AsyncClient).selectObjectContent(request.capture(), any(S3SelectResponseHandler.class));
		assertThat(request.getValue().key(), equalTo(key));
		assertThat(request.getValue().bucket(), equalTo(bucketName));
		assertThat(request.getValue().expectedBucketOwner(), nullValue());
		assertThat(request.getValue().expression(), equalTo(s3SourceConfig.getS3SelectOptions().getQueryStatement()));
		assertThat(request.getValue().inputSerialization().json(), equalTo(jsonInput));
		assertThat(request.getValue().outputSerialization().json(), equalTo(jsonOut));
	}

	@Test
	public void selectObjectFromS3_csv_test_correct_not_null_response() throws Exception {
		setupForS3Select("csv");

		when(s3AsyncClient.selectObjectContent(any(SelectObjectContentRequest.class),
				any(S3SelectResponseHandler.class))).thenReturn(selectObjectContent);

		s3SelectObjectWorker.selectObjectFromS3(s3ObjectReference, bufferAccumulator);
		final ArgumentCaptor<S3SelectResponseHandler> responseHandler = ArgumentCaptor
				.forClass(S3SelectResponseHandler.class);
		verify(s3AsyncClient).selectObjectContent(any(SelectObjectContentRequest.class), responseHandler.capture());
		assertThat(responseHandler.getValue(), notNullValue());
		assertThat(responseHandler.getValue().getReceivedEvents(), notNullValue());
	}

	@Test
	public void selectObjectFromS3_csv_test_throws_Internal_Server_Error() throws Exception {
		
		setupForS3Select("csv");
		String requestId = UUID.randomUUID().toString();
		String externalRequestId = UUID.randomUUID().toString();
		S3Exception expected = (S3Exception) S3Exception.builder().message("Interal Server Error")
				.extendedRequestId(externalRequestId).requestId(requestId).statusCode(500).build();

		doThrow(expected).when(s3AsyncClient).selectObjectContent(any(SelectObjectContentRequest.class),
				any(S3SelectResponseHandler.class));
		S3Exception actual = assertThrows(S3Exception.class,
				() -> s3SelectObjectWorker.selectObjectFromS3(s3ObjectReference, bufferAccumulator));
		assertThat(expected, equalTo(actual));
		assertThat(requestId, equalTo(actual.requestId()));
		assertThat(externalRequestId, equalTo(actual.extendedRequestId()));
		assertThat(expected.getMessage(), equalTo(actual.getMessage()));
		verify(s3SelectFailedCounter).increment();
		
	}

	@Test
	public void selectObjectFromS3_parquet_test_throws_400() {
		setupForS3Select("json");
		String requestId = UUID.randomUUID().toString();
		String externalRequestId = UUID.randomUUID().toString();
		S3Exception expected = (S3Exception) S3Exception.builder().message("invalid query")
				.extendedRequestId(externalRequestId).requestId(requestId).statusCode(400).build();
		doThrow(expected).when(s3AsyncClient).selectObjectContent(any(SelectObjectContentRequest.class),
				any(S3SelectResponseHandler.class));
		S3Exception actual = assertThrows(S3Exception.class,
				() -> s3SelectObjectWorker.selectObjectFromS3(s3ObjectReference, bufferAccumulator));
		assertThat(expected, equalTo(actual));
		assertThat(requestId, equalTo(actual.requestId()));
		assertThat(externalRequestId, equalTo(actual.extendedRequestId()));
		assertThat(expected.getMessage(), equalTo(actual.getMessage()));
		verify(s3SelectFailedCounter).increment();
	}

	@Test
	public void selectObjectFromS3_parquet_test_throws_500_retry() {
		setupForS3Select("parquet");
		String requestId = UUID.randomUUID().toString();
		String externalRequestId = UUID.randomUUID().toString();
		S3Exception expected = (S3Exception) S3Exception.builder().message("invalid query")
				.extendedRequestId(externalRequestId).requestId(requestId).statusCode(500).build();
		doThrow(new RuntimeException(expected)).when(s3AsyncClient).selectObjectContent(any(SelectObjectContentRequest.class),
				any(S3SelectResponseHandler.class));
		RuntimeException actual = assertThrows(RuntimeException.class,
				() -> s3SelectObjectWorker.selectObjectFromS3(s3ObjectReference, bufferAccumulator));
		assertThat(expected, equalTo(actual.getCause()));
		assertThat(requestId, equalTo(((S3Exception)actual.getCause()).requestId()));
		assertThat(externalRequestId, equalTo(((S3Exception)actual.getCause()).extendedRequestId()));
		assertThat(expected.getMessage(), equalTo(((S3Exception)actual.getCause()).getMessage()));
		verify(s3SelectFailedCounter).increment();
	}
	
	@Test
	public void selectObjectFromS3_csv_test_throws_500_Other_Exception() {
		setupForS3Select("csv");
		
		RuntimeException exception = mock(RuntimeException.class);
		when(exception.getMessage()).thenReturn("unable to read data from steams");
		
		doThrow(exception).when(s3AsyncClient).selectObjectContent(any(SelectObjectContentRequest.class),
				any(S3SelectResponseHandler.class));
		RuntimeException actual = assertThrows(RuntimeException.class,
				() -> s3SelectObjectWorker.selectObjectFromS3(s3ObjectReference, bufferAccumulator));
		assertThat(exception, equalTo(actual));
		assertThat(exception.getMessage(), equalTo(actual.getMessage()));
		verify(s3SelectFailedCounter).increment();
	}
	
	@Test
	public void selectObjectFromS3_csv_test_with_correct_response() throws Exception {
		setupForS3Select("csv");
		
		responseHand = mock(S3SelectResponseHandler.class);
		
		receivedEvents.add(contentEventStream);
		lenient().when(responseHand.getReceivedEvents()).thenReturn(receivedEvents);
		lenient().when(receivedEvents.size()).thenReturn(10);
//		private List<SelectObjectContentEventStream> receivedEvents = new ArrayList<>();
		when(s3AsyncClient.selectObjectContent(any(SelectObjectContentRequest.class),any(S3SelectResponseHandler.class))).thenReturn(selectObjectContent);
		s3SelectObjectWorker.selectObjectFromS3(s3ObjectReference, bufferAccumulator);
		
	}
}