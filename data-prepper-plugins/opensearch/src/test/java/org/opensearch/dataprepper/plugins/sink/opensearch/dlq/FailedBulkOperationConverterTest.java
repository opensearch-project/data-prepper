package org.opensearch.dataprepper.plugins.sink.opensearch.dlq;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch._types.ErrorCause;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.opensearch.core.bulk.CreateOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.SerializedJson;
import org.opensearch.dataprepper.plugins.sink.opensearch.BulkOperationWrapper;
import org.opensearch.rest.RestStatus;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FailedBulkOperationConverterTest {

    private SerializedJson document;
    private Object expectedDoc;
    private String testIndex;
    private String testId;
    private String pipelineName;
    private String pluginName;
    private BulkOperation bulkOperation;
    private BulkResponseItem bulkResponseItem;
    private String errorReason;
    private Throwable failure;
    private String failureMessage;

    private FailedBulkOperationConverter converter;

    @BeforeEach
    public void setup() {
        generateRandomDocument();
        testIndex = UUID.randomUUID().toString();
        testId = UUID.randomUUID().toString();
        pipelineName = UUID.randomUUID().toString();
        pluginName = UUID.randomUUID().toString();
        errorReason = UUID.randomUUID().toString();

        failureMessage = UUID.randomUUID().toString();
        failure = new Exception(failureMessage);
        bulkOperation = mock(BulkOperation.class);

        final IndexOperation indexOperation = mock(IndexOperation.class);
        when(indexOperation.index()).thenReturn(testIndex);
        when(indexOperation.document()).thenReturn(document);
        when(indexOperation.id()).thenReturn(testId);

        final CreateOperation createOperation = mock(CreateOperation.class);
        when(createOperation.index()).thenReturn(testIndex);
        when(createOperation.document()).thenReturn(document);
        when(createOperation.id()).thenReturn(testId);

        when(bulkOperation.isIndex()).thenReturn(true);
        when(bulkOperation.index()).thenReturn(indexOperation);
        when(bulkOperation.isCreate()).thenReturn(false);
        when(bulkOperation.create()).thenReturn(createOperation);

        final ErrorCause errorCause = mock(ErrorCause.class);
        bulkResponseItem = mock(BulkResponseItem.class);
        when(bulkResponseItem.status()).thenReturn(RestStatus.INTERNAL_SERVER_ERROR.getStatus());
        when(bulkResponseItem.error()).thenReturn(errorCause);
        when(errorCause.reason()).thenReturn(errorReason);

        converter = new FailedBulkOperationConverter(pipelineName, pluginName);
    }

    @Test
    public void testConvertToDlqObject() {

        final FailedBulkOperation testData = FailedBulkOperation.builder()
            .withBulkOperation(new BulkOperationWrapper(bulkOperation))
            .withBulkResponseItem(bulkResponseItem)
            .withFailure(failure)
            .build();

        final DlqObject result = converter.convertToDlqObject(testData);

        validateResponse(result, errorReason);
    }

    @Test
    public void testConvertActionCreateToDlqObject() {
        when(bulkOperation.isIndex()).thenReturn(false);
        when(bulkOperation.isCreate()).thenReturn(true);

        final FailedBulkOperation testData = FailedBulkOperation.builder()
                .withBulkOperation(new BulkOperationWrapper(bulkOperation))
                .withBulkResponseItem(bulkResponseItem)
                .withFailure(failure)
                .build();

        final DlqObject result = converter.convertToDlqObject(testData);

        validateResponse(result, errorReason);
    }

    @Test
    public void testConvertToDlqObjectWithOnlyFailure() {
        final FailedBulkOperation testData = FailedBulkOperation.builder()
            .withBulkOperation(new BulkOperationWrapper(bulkOperation))
            .withFailure(failure)
            .build();

        final DlqObject result = converter.convertToDlqObject(testData);

        validateResponse(result, failureMessage);
    }

    @Test
    public void testConvertToDlqObjectWithOnlyBulkResponseItem() {

        final FailedBulkOperation testData = FailedBulkOperation.builder()
            .withBulkOperation(new BulkOperationWrapper(bulkOperation))
            .withBulkResponseItem(bulkResponseItem)
            .build();

        final DlqObject result = converter.convertToDlqObject(testData);

        validateResponse(result, errorReason);
    }

    private void validateResponse(final DlqObject result, final String expectedErrorMessage) {
        assertThat(result, is(notNullValue()));
        assertThat(result.getPipelineName(), is(equalTo(pipelineName)));
        assertThat(result.getPluginId(), is(equalTo(pluginName)));
        assertThat(result.getPluginName(), is(equalTo(pluginName)));
        final Object failedData = result.getFailedData();
        assertThat(failedData, is(notNullValue()));
        assertThat(failedData, isA(FailedDlqData.class));
        final FailedDlqData failedDlqData = (FailedDlqData) failedData;
        Map<String, Object> doc = (Map<String, Object>) failedDlqData.getDocument();
        assertThat(doc, is(equalTo(expectedDoc)));
        assertThat(failedDlqData.getIndex(), is(equalTo(testIndex)));
        assertThat(failedDlqData.getIndexId(), is(equalTo(testId)));
        assertThat(failedDlqData.getMessage(), is(equalTo(expectedErrorMessage)));
    }

    private void generateRandomDocument() {

        final String key1 = UUID.randomUUID().toString();
        final String key2 = UUID.randomUUID().toString();
        final String value1 = UUID.randomUUID().toString();
        final String value2 = UUID.randomUUID().toString();

        expectedDoc = ImmutableMap.of(key1, value1, key2, value2);

        final String jsonString = String.format("{\"%s\": \"%s\", \"%s\": \"%s\"}", key1, value1, key2, value2);

        document = SerializedJson.fromStringAndOptionals(jsonString, null, null, null);
    }


}
