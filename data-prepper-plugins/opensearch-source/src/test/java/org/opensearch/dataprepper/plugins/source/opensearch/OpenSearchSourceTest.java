/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.plugins.source.opensearch.metrics.OpenSearchSourcePluginMetrics;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.OpenSearchClientFactory;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.SearchAccessor;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.SearchAccessorStrategy;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class OpenSearchSourceTest {

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private OpenSearchSourceConfiguration openSearchSourceConfiguration;

    @Mock
    private OpenSearchService openSearchService;

    @Mock
    private OpenSearchClientFactory openSearchClientFactory;

    @Mock
    private SearchAccessorStrategy searchAccessorStrategy;

    @Mock
    private SearchAccessor searchAccessor;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private OpenSearchSourcePluginMetrics openSearchSourcePluginMetrics;

    @Mock
    private SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator;

    @Mock
    private PluginConfigObservable pluginConfigObservable;

    private OpenSearchSource createObjectUnderTest() {
        return new OpenSearchSource(
                openSearchSourceConfiguration, awsCredentialsSupplier, acknowledgementSetManager,
                pluginMetrics, pluginConfigObservable);
    }

    @Test
    void start_with_null_buffer_throws_IllegalStateException() {
        assertThrows(IllegalStateException.class, () -> createObjectUnderTest().start(null));
    }

    @Test
    void start_with_non_null_buffer_does_not_throw() {

        when(searchAccessorStrategy.getSearchAccessor()).thenReturn(searchAccessor);
        doNothing().when(openSearchService).start();

        final OpenSearchSource objectUnderTest = createObjectUnderTest();
        objectUnderTest.setSourceCoordinator(sourceCoordinator);

        try (final MockedStatic<SearchAccessorStrategy> searchAccessorStrategyMockedStatic = mockStatic(SearchAccessorStrategy.class);
             final MockedStatic<OpenSearchClientFactory> openSearchClientFactoryMockedStatic = mockStatic(OpenSearchClientFactory.class);
             final MockedStatic<OpenSearchSourcePluginMetrics> openSearchSourcePluginMetricsMockedStatic = mockStatic(OpenSearchSourcePluginMetrics.class);
             final MockedStatic<OpenSearchService> openSearchServiceMockedStatic = mockStatic(OpenSearchService.class)) {
            openSearchClientFactoryMockedStatic.when(() -> OpenSearchClientFactory.create(awsCredentialsSupplier)).thenReturn(openSearchClientFactory);
            searchAccessorStrategyMockedStatic.when(() -> SearchAccessorStrategy.create(
                    openSearchSourceConfiguration, openSearchClientFactory, pluginConfigObservable)).thenReturn(searchAccessorStrategy);
            openSearchSourcePluginMetricsMockedStatic.when(() -> OpenSearchSourcePluginMetrics.create(pluginMetrics)).thenReturn(openSearchSourcePluginMetrics);

            openSearchServiceMockedStatic.when(() -> OpenSearchService.createOpenSearchService(searchAccessor, sourceCoordinator, openSearchSourceConfiguration, buffer, acknowledgementSetManager, openSearchSourcePluginMetrics))
                    .thenReturn(openSearchService);

            objectUnderTest.start(buffer);
        }
    }
}
