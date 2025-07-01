package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.MsearchRequest;
import org.opensearch.client.opensearch.core.MsearchResponse;
import org.opensearch.client.opensearch.core.msearch.MultiSearchItem;
import org.opensearch.client.opensearch.core.msearch.MultiSearchResponseItem;
import org.opensearch.client.opensearch.core.search.Hit;
import org.opensearch.client.opensearch.core.search.HitsMetadata;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.sink.opensearch.BulkOperationWrapper;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.ExistingDocumentQueryManager.DOCUMENTS_CURRENTLY_BEING_QUERIED;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.ExistingDocumentQueryManager.DUPLICATE_EVENTS_IN_QUERY_MANAGER;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.ExistingDocumentQueryManager.EVENTS_ADDED_FOR_QUERYING;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.ExistingDocumentQueryManager.EVENTS_DROPPED_AND_RELEASED;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.ExistingDocumentQueryManager.EVENTS_RETURNED_FOR_INDEXING;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.ExistingDocumentQueryManager.POTENTIAL_DUPLICATES;

@ExtendWith(MockitoExtension.class)
public class ExistingDocumentQueryManagerTest {

    @Mock
    private IndexConfiguration indexConfiguration;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private OpenSearchClient openSearchClient;

    @Mock
    private Counter eventsAddedForQuerying;

    @Mock
    private Counter eventsDroppedAndReleased;

    @Mock
    private Counter eventsReturnedForIndexing;

    @Mock
    private Counter duplicateEventsAddedToQueryManager;

    @Mock
    private Counter potentialDuplicates;

    @Mock
    private AtomicInteger documentsCurrentlyQueried;

    private String queryTerm;

    @BeforeEach
    void setup() {
        when(pluginMetrics.counter(EVENTS_ADDED_FOR_QUERYING)).thenReturn(eventsAddedForQuerying);
        when(pluginMetrics.counter(EVENTS_DROPPED_AND_RELEASED)).thenReturn(eventsDroppedAndReleased);
        when(pluginMetrics.counter(EVENTS_RETURNED_FOR_INDEXING)).thenReturn(eventsReturnedForIndexing);
        when(pluginMetrics.counter(DUPLICATE_EVENTS_IN_QUERY_MANAGER)).thenReturn(duplicateEventsAddedToQueryManager);
        when(pluginMetrics.gauge(eq(DOCUMENTS_CURRENTLY_BEING_QUERIED), any(AtomicInteger.class), any())).thenReturn(documentsCurrentlyQueried);
        when(pluginMetrics.counter(POTENTIAL_DUPLICATES)).thenReturn(potentialDuplicates);
        queryTerm = UUID.randomUUID().toString();
        when(indexConfiguration.getQueryTerm()).thenReturn(queryTerm);
    }

    private ExistingDocumentQueryManager createObjectUnderTest() {
        return new ExistingDocumentQueryManager(indexConfiguration, pluginMetrics, openSearchClient);
    }

    @Test
    void add_bulk_operation_and_found_in_query_drops_and_releases_event() throws IOException {
        final BulkOperationWrapper bulkOperationWrapper = mock(BulkOperationWrapper.class);
        final String index = UUID.randomUUID().toString();
        final String termValue = UUID.randomUUID().toString();
        when(bulkOperationWrapper.getTermValue()).thenReturn(termValue);
        when(bulkOperationWrapper.getIndex()).thenReturn(index);

        final MsearchResponse<ObjectNode> msearchResponse = mock(MsearchResponse.class);
        final MultiSearchResponseItem<ObjectNode> responseItem = mock(MultiSearchResponseItem.class);
        when(responseItem.isFailure()).thenReturn(false);

        final MultiSearchItem<ObjectNode> multiSearchItem = mock(MultiSearchItem.class);
        final HitsMetadata<ObjectNode> hitsMetadata = mock(HitsMetadata.class);
        final Hit<ObjectNode> hit = mock(Hit.class);
        when(hit.index()).thenReturn(index);

        final ObjectNode objectNode = mock(ObjectNode.class);
        final JsonNode jsonNode = mock(JsonNode.class);
        when(jsonNode.textValue()).thenReturn(termValue);
        when(objectNode.findValue(queryTerm)).thenReturn(jsonNode);
        when(hit.source()).thenReturn(objectNode);

        when(multiSearchItem.hits()).thenReturn(hitsMetadata);
        when(hitsMetadata.hits()).thenReturn(List.of(hit));

        when(responseItem.result()).thenReturn(multiSearchItem);

        when(msearchResponse.responses()).thenReturn(List.of(responseItem));

        when(openSearchClient.msearch(any(MsearchRequest.class), eq(ObjectNode.class)))
                .thenReturn(msearchResponse);

        final ExistingDocumentQueryManager objectUnderTest = createObjectUnderTest();

        objectUnderTest.addBulkOperation(bulkOperationWrapper);

        objectUnderTest.runQueryLoop();

        verify(eventsDroppedAndReleased).increment();
        verify(eventsAddedForQuerying).increment();
        verify(documentsCurrentlyQueried).incrementAndGet();
        verify(documentsCurrentlyQueried).decrementAndGet();
        verify(bulkOperationWrapper).releaseEventHandle(true);

        verifyNoMoreInteractions(indexConfiguration);
    }

    @Test
    void add_bulk_operation_and_not_found_in_query_returns_as_ready_to_ingest() throws IOException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        when(indexConfiguration.getQueryDuration()).thenReturn(Duration.ofMillis(1));

        final BulkOperationWrapper bulkOperationWrapper = mock(BulkOperationWrapper.class);
        final String index = UUID.randomUUID().toString();
        final String termValue = UUID.randomUUID().toString();
        when(bulkOperationWrapper.getTermValue()).thenReturn(termValue);
        when(bulkOperationWrapper.getIndex()).thenReturn(index);

        final MsearchResponse<ObjectNode> msearchResponse = mock(MsearchResponse.class);
        final MultiSearchResponseItem<ObjectNode> responseItem = mock(MultiSearchResponseItem.class);
        when(responseItem.isFailure()).thenReturn(false);

        final MultiSearchItem<ObjectNode> multiSearchItem = mock(MultiSearchItem.class);
        final HitsMetadata<ObjectNode> hitsMetadata = mock(HitsMetadata.class);
        final Hit<ObjectNode> hit = mock(Hit.class);
        when(hit.index()).thenReturn(index);

        final ObjectNode objectNode = mock(ObjectNode.class);
        final JsonNode jsonNode = mock(JsonNode.class);
        when(jsonNode.textValue()).thenReturn(UUID.randomUUID().toString());
        when(objectNode.findValue(queryTerm)).thenReturn(jsonNode);
        when(hit.source()).thenReturn(objectNode);

        when(multiSearchItem.hits()).thenReturn(hitsMetadata);
        when(hitsMetadata.hits()).thenReturn(List.of(hit));

        when(responseItem.result()).thenReturn(multiSearchItem);

        when(msearchResponse.responses()).thenReturn(List.of(responseItem));

        when(openSearchClient.msearch(any(MsearchRequest.class), eq(ObjectNode.class)))
                .thenReturn(msearchResponse);

        final ExistingDocumentQueryManager objectUnderTest = createObjectUnderTest();

        ReflectivelySetField.setField(ExistingDocumentQueryManager.class, objectUnderTest, "lastQueryTime", Instant.now().plusMillis(100));

        objectUnderTest.addBulkOperation(bulkOperationWrapper);

        Thread.sleep(20);

        objectUnderTest.runQueryLoop();

        final Set<BulkOperationWrapper> readyForIndexing = objectUnderTest.getAndClearBulkOperationsReadyToIndex();
        assertThat(readyForIndexing, notNullValue());
        assertThat(readyForIndexing.size(), equalTo(1));
        assertThat(readyForIndexing.contains(bulkOperationWrapper), equalTo(true));

        verifyNoMoreInteractions(eventsDroppedAndReleased);
        verify(eventsAddedForQuerying).increment();
        verify(documentsCurrentlyQueried).incrementAndGet();
        verify(eventsReturnedForIndexing).increment(1);
        verify(documentsCurrentlyQueried).decrementAndGet();

        final Set<BulkOperationWrapper> nothingReadyForIndexing = objectUnderTest.getAndClearBulkOperationsReadyToIndex();
        assertThat(nothingReadyForIndexing.isEmpty(), equalTo(true));
        verify(eventsReturnedForIndexing).increment(0);

        final ArgumentCaptor<MsearchRequest> msearchRequestArgumentCaptor = ArgumentCaptor.forClass(MsearchRequest.class);
        verify(openSearchClient).msearch(msearchRequestArgumentCaptor.capture(), eq(ObjectNode.class));

        final MsearchRequest msearchRequest = msearchRequestArgumentCaptor.getValue();
        assertThat(msearchRequest.searches().size(), equalTo(1));
        assertThat(msearchRequest.searches().get(0), notNullValue());
        assertThat(msearchRequest.searches().get(0).header(), notNullValue());
        assertThat(msearchRequest.searches().get(0).header().index(), notNullValue());
        assertThat(msearchRequest.searches().get(0).header().index().size(), equalTo(1));
        assertThat(msearchRequest.searches().get(0).header().index().get(0), equalTo(index));
        assertThat(msearchRequest.searches().get(0).body(), notNullValue());
        assertThat(msearchRequest.searches().get(0).body().source(), notNullValue());
        assertThat(msearchRequest.searches().get(0).body().source().filter(), notNullValue());
        assertThat(msearchRequest.searches().get(0).body().source().filter().includes(), notNullValue());
        assertThat(msearchRequest.searches().get(0).body().source().filter().includes().size(), equalTo(1));
        assertThat(msearchRequest.searches().get(0).body().source().filter().includes().get(0), equalTo(queryTerm));
        assertThat(msearchRequest.searches().get(0).body().query(), notNullValue());
        assertThat(msearchRequest.searches().get(0).body().query().terms(), notNullValue());
        assertThat(msearchRequest.searches().get(0).body().query().terms().terms(), notNullValue());
        assertThat(msearchRequest.searches().get(0).body().query().terms().terms().value(), notNullValue());
        assertThat(msearchRequest.searches().get(0).body().query().terms().terms().value().size(), equalTo(1));
        assertThat(msearchRequest.searches().get(0).body().query().terms().terms().value().get(0), notNullValue());
        assertThat(msearchRequest.searches().get(0).body().query().terms().terms().value().get(0).stringValue(), equalTo(termValue));
    }

    @Test
    void add_bulk_operation_for_same_term_value_increments_duplicate_events_metric() {
        final BulkOperationWrapper bulkOperationWrapper = mock(BulkOperationWrapper.class);
        final String index = UUID.randomUUID().toString();
        final String termValue = UUID.randomUUID().toString();
        when(bulkOperationWrapper.getTermValue()).thenReturn(termValue);
        when(bulkOperationWrapper.getIndex()).thenReturn(index);

        final ExistingDocumentQueryManager objectUnderTest = createObjectUnderTest();

        objectUnderTest.addBulkOperation(bulkOperationWrapper);

        objectUnderTest.addBulkOperation(bulkOperationWrapper);

        verify(duplicateEventsAddedToQueryManager).increment();
        verify(documentsCurrentlyQueried).incrementAndGet();
    }

    @Test
    void query_response_with_two_documents_with_same_term_value_tracks_duplicate_document() throws IOException {
        final BulkOperationWrapper bulkOperationWrapper = mock(BulkOperationWrapper.class);
        final String index = UUID.randomUUID().toString();
        final String termValue = UUID.randomUUID().toString();
        when(bulkOperationWrapper.getTermValue()).thenReturn(termValue);
        when(bulkOperationWrapper.getIndex()).thenReturn(index);

        final MsearchResponse<ObjectNode> msearchResponse = mock(MsearchResponse.class);
        final MultiSearchResponseItem<ObjectNode> responseItem = mock(MultiSearchResponseItem.class);
        when(responseItem.isFailure()).thenReturn(false);

        final MultiSearchItem<ObjectNode> multiSearchItem = mock(MultiSearchItem.class);
        final HitsMetadata<ObjectNode> hitsMetadata = mock(HitsMetadata.class);
        final Hit<ObjectNode> hit = mock(Hit.class);
        when(hit.index()).thenReturn(index);

        final ObjectNode objectNode = mock(ObjectNode.class);
        final JsonNode jsonNode = mock(JsonNode.class);
        when(jsonNode.textValue()).thenReturn(termValue);
        when(objectNode.findValue(queryTerm)).thenReturn(jsonNode);
        when(hit.source()).thenReturn(objectNode);

        final Hit<ObjectNode> duplicateHit = mock(Hit.class);
        when(duplicateHit.index()).thenReturn(index);
        when(duplicateHit.id()).thenReturn(UUID.randomUUID().toString());

        when(jsonNode.textValue()).thenReturn(termValue);
        when(objectNode.findValue(queryTerm)).thenReturn(jsonNode);
        when(duplicateHit.source()).thenReturn(objectNode);

        when(multiSearchItem.hits()).thenReturn(hitsMetadata);
        when(hitsMetadata.hits()).thenReturn(List.of(hit, duplicateHit));

        when(responseItem.result()).thenReturn(multiSearchItem);

        when(msearchResponse.responses()).thenReturn(List.of(responseItem));

        when(openSearchClient.msearch(any(MsearchRequest.class), eq(ObjectNode.class)))
                .thenReturn(msearchResponse);

        final ExistingDocumentQueryManager objectUnderTest = createObjectUnderTest();

        objectUnderTest.addBulkOperation(bulkOperationWrapper);

        objectUnderTest.runQueryLoop();

        verify(openSearchClient).msearch(any(MsearchRequest.class), eq(ObjectNode.class));
        verify(potentialDuplicates).increment();
        verifyNoMoreInteractions(openSearchClient);

    }
}
