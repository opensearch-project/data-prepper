/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.documentdb;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.mongo.configuration.CollectionConfig;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.mongo.export.ExportScheduler;
import org.opensearch.dataprepper.plugins.mongo.export.ExportWorker;
import org.opensearch.dataprepper.plugins.mongo.stream.StreamScheduler;
import org.opensearch.dataprepper.plugins.mongo.utils.DocumentDBSourceAggregateMetrics;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.mongo.documentdb.MongoTasksRefresher.CREDENTIALS_CHANGED;
import static org.opensearch.dataprepper.plugins.mongo.documentdb.MongoTasksRefresher.EXECUTOR_REFRESH_ERRORS;

@ExtendWith(MockitoExtension.class)
class MongoTasksRefresherTest {
    private static final String TEST_USERNAME = "test_user";
    private static final String TEST_PASSWORD = "test_password";
    private final String S3_PATH_PREFIX = UUID.randomUUID().toString();

    @Mock
    private EnhancedSourceCoordinator enhancedSourceCoordinator;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private Function<Integer, ExecutorService> executorServiceFunction;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private MongoDBSourceConfig sourceConfig;

    @Mock
    private MongoDBSourceConfig.AuthenticationConfig credentialsConfig;

    @Mock
    private ExecutorService executorService;

    @Mock
    private CollectionConfig collectionConfig;

    @Mock
    private DocumentDBSourceAggregateMetrics documentDBSourceAggregateMetrics;

    @Mock
    private Counter credentialsChangeCounter;

    @Mock
    private Counter executorRefreshErrorsCounter;

    private MongoTasksRefresher createObjectUnderTest() {
        return new MongoTasksRefresher(
                buffer, enhancedSourceCoordinator, pluginMetrics, acknowledgementSetManager,
                executorServiceFunction, S3_PATH_PREFIX, documentDBSourceAggregateMetrics);
    }

    @BeforeEach
    void setUp() {
        lenient().when(executorServiceFunction.apply(anyInt())).thenReturn(executorService);
        lenient().when(sourceConfig.getCollections()).thenReturn(List.of(collectionConfig));
        lenient().when(collectionConfig.isExport()).thenReturn(true);
        lenient().when(collectionConfig.isStream()).thenReturn(true);
    }

    @Test
    void testUpdateWithBasicAuthUnchanged() {
        final MongoTasksRefresher objectUnderTest = createObjectUnderTest();
        objectUnderTest.initialize(sourceConfig);
        verify(executorServiceFunction).apply(eq(3));
        when(sourceConfig.getAuthenticationConfig()).thenReturn(credentialsConfig);
        when(credentialsConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(credentialsConfig.getPassword()).thenReturn(TEST_PASSWORD);
        final MongoDBSourceConfig newSourceConfig = mock(MongoDBSourceConfig.class);
        final MongoDBSourceConfig.AuthenticationConfig newCredentialsConfig = mock(
                MongoDBSourceConfig.AuthenticationConfig.class);
        when(newSourceConfig.getAuthenticationConfig()).thenReturn(newCredentialsConfig);
        when(newCredentialsConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(newCredentialsConfig.getPassword()).thenReturn(TEST_PASSWORD);
        objectUnderTest.update(newSourceConfig);
        verify(executorService).submit(any(ExportScheduler.class));
        verify(executorService).submit(any(ExportWorker.class));
        verify(executorService).submit(any(StreamScheduler.class));
        verifyNoMoreInteractions(executorServiceFunction);
    }

    @Test
    void testUpdateWithUsernameChangedAndBothStreamAndExportEnabled() {
        when(pluginMetrics.counter(CREDENTIALS_CHANGED)).thenReturn(credentialsChangeCounter);
        final MongoTasksRefresher objectUnderTest = createObjectUnderTest();
        objectUnderTest.initialize(sourceConfig);
        verify(executorServiceFunction).apply(eq(3));
        when(sourceConfig.getAuthenticationConfig()).thenReturn(credentialsConfig);
        when(credentialsConfig.getUsername()).thenReturn(TEST_USERNAME);
        final MongoDBSourceConfig newSourceConfig = mock(MongoDBSourceConfig.class);
        when(newSourceConfig.getCollections()).thenReturn(List.of(collectionConfig));
        final MongoDBSourceConfig.AuthenticationConfig newCredentialsConfig = mock(
                MongoDBSourceConfig.AuthenticationConfig.class);
        when(newSourceConfig.getAuthenticationConfig()).thenReturn(newCredentialsConfig);
        when(newCredentialsConfig.getUsername()).thenReturn(TEST_USERNAME + "_changed");
        final ExecutorService newExecutorService = mock(ExecutorService.class);
        when(executorServiceFunction.apply(anyInt())).thenReturn(newExecutorService);
        objectUnderTest.update(newSourceConfig);
        verify(credentialsChangeCounter).increment();
        verify(executorService).shutdownNow();
        verify(executorService).submit(any(ExportScheduler.class));
        verify(executorService).submit(any(ExportWorker.class));
        verify(executorService).submit(any(StreamScheduler.class));
        verify(executorServiceFunction, times(2)).apply(eq(3));
    }

    @Test
    void testUpdateWithPasswordChangedAndBothStreamAndExportEnabled() {
        when(pluginMetrics.counter(CREDENTIALS_CHANGED)).thenReturn(credentialsChangeCounter);
        final MongoTasksRefresher objectUnderTest = createObjectUnderTest();
        objectUnderTest.initialize(sourceConfig);
        verify(executorServiceFunction).apply(eq(3));
        when(sourceConfig.getAuthenticationConfig()).thenReturn(credentialsConfig);
        when(credentialsConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(credentialsConfig.getPassword()).thenReturn(TEST_PASSWORD);
        final MongoDBSourceConfig newSourceConfig = mock(MongoDBSourceConfig.class);
        when(newSourceConfig.getCollections()).thenReturn(List.of(collectionConfig));
        final MongoDBSourceConfig.AuthenticationConfig newCredentialsConfig = mock(
                MongoDBSourceConfig.AuthenticationConfig.class);
        when(newSourceConfig.getAuthenticationConfig()).thenReturn(newCredentialsConfig);
        when(newCredentialsConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(newCredentialsConfig.getPassword()).thenReturn(TEST_PASSWORD  + "_changed");
        final ExecutorService newExecutorService = mock(ExecutorService.class);
        when(executorServiceFunction.apply(anyInt())).thenReturn(newExecutorService);
        objectUnderTest.update(newSourceConfig);
        verify(credentialsChangeCounter).increment();
        verify(executorService).shutdownNow();
        verify(executorService).submit(any(ExportScheduler.class));
        verify(executorService).submit(any(ExportWorker.class));
        verify(executorService).submit(any(StreamScheduler.class));
        verify(executorServiceFunction, times(2)).apply(eq(3));
    }

    @Test
    void testUpdateWithPasswordChangedAndOnlyExportEnabled() {
        when(collectionConfig.isStream()).thenReturn(false);
        when(pluginMetrics.counter(CREDENTIALS_CHANGED)).thenReturn(credentialsChangeCounter);
        final MongoTasksRefresher objectUnderTest = createObjectUnderTest();
        objectUnderTest.initialize(sourceConfig);
        verify(executorServiceFunction).apply(eq(2));
        when(sourceConfig.getAuthenticationConfig()).thenReturn(credentialsConfig);
        when(credentialsConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(credentialsConfig.getPassword()).thenReturn(TEST_PASSWORD);
        final MongoDBSourceConfig newSourceConfig = mock(MongoDBSourceConfig.class);
        when(newSourceConfig.getCollections()).thenReturn(List.of(collectionConfig));
        final MongoDBSourceConfig.AuthenticationConfig newCredentialsConfig = mock(
                MongoDBSourceConfig.AuthenticationConfig.class);
        when(newSourceConfig.getAuthenticationConfig()).thenReturn(newCredentialsConfig);
        when(newCredentialsConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(newCredentialsConfig.getPassword()).thenReturn(TEST_PASSWORD  + "_changed");
        final ExecutorService newExecutorService = mock(ExecutorService.class);
        when(executorServiceFunction.apply(anyInt())).thenReturn(newExecutorService);
        objectUnderTest.update(newSourceConfig);
        verify(credentialsChangeCounter).increment();
        verify(executorService).shutdownNow();
        verify(newExecutorService).submit(any(ExportScheduler.class));
        verify(newExecutorService).submit(any(ExportWorker.class));
        verify(executorServiceFunction, times(2)).apply(eq(2));
    }

    @Test
    void testUpdateWithPasswordChangedAndOnlyStreamEnabled() {
        when(collectionConfig.isExport()).thenReturn(false);
        when(pluginMetrics.counter(CREDENTIALS_CHANGED)).thenReturn(credentialsChangeCounter);
        final MongoTasksRefresher objectUnderTest = createObjectUnderTest();
        objectUnderTest.initialize(sourceConfig);
        verify(executorServiceFunction).apply(eq(1));
        when(sourceConfig.getAuthenticationConfig()).thenReturn(credentialsConfig);
        when(credentialsConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(credentialsConfig.getPassword()).thenReturn(TEST_PASSWORD);
        final MongoDBSourceConfig newSourceConfig = mock(MongoDBSourceConfig.class);
        when(newSourceConfig.getCollections()).thenReturn(List.of(collectionConfig));
        final MongoDBSourceConfig.AuthenticationConfig newCredentialsConfig = mock(
                MongoDBSourceConfig.AuthenticationConfig.class);
        when(newSourceConfig.getAuthenticationConfig()).thenReturn(newCredentialsConfig);
        when(newCredentialsConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(newCredentialsConfig.getPassword()).thenReturn(TEST_PASSWORD  + "_changed");
        final ExecutorService newExecutorService = mock(ExecutorService.class);
        when(executorServiceFunction.apply(anyInt())).thenReturn(newExecutorService);
        objectUnderTest.update(newSourceConfig);
        verify(credentialsChangeCounter).increment();
        verify(executorService).shutdownNow();
        verify(executorService).submit(any(StreamScheduler.class));
        verify(executorServiceFunction, times(2)).apply(eq(1));
    }

    @Test
    void testGetAfterUpdateClientFailure() {
        when(pluginMetrics.counter(CREDENTIALS_CHANGED)).thenReturn(credentialsChangeCounter);
        when(pluginMetrics.counter(EXECUTOR_REFRESH_ERRORS)).thenReturn(executorRefreshErrorsCounter);
        final MongoTasksRefresher objectUnderTest = createObjectUnderTest();
        objectUnderTest.initialize(sourceConfig);
        verify(executorServiceFunction).apply(eq(3));
        when(sourceConfig.getAuthenticationConfig()).thenReturn(credentialsConfig);
        when(credentialsConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(credentialsConfig.getPassword()).thenReturn(TEST_PASSWORD);
        final MongoDBSourceConfig newSourceConfig = mock(MongoDBSourceConfig.class);
        final MongoDBSourceConfig.AuthenticationConfig newCredentialsConfig = mock(
                MongoDBSourceConfig.AuthenticationConfig.class);
        when(newSourceConfig.getAuthenticationConfig()).thenReturn(newCredentialsConfig);
        when(newCredentialsConfig.getUsername()).thenReturn(TEST_USERNAME);
        when(newCredentialsConfig.getPassword()).thenReturn(TEST_PASSWORD  + "_changed");
        doThrow(RuntimeException.class).when(executorService).shutdownNow();
        objectUnderTest.update(newSourceConfig);
        verify(credentialsChangeCounter).increment();
        verify(executorRefreshErrorsCounter).increment();
        verifyNoMoreInteractions(executorServiceFunction);
    }

    @Test
    void testTaskRefreshWithNullS3PathPrefix() {
        assertThrows(IllegalArgumentException.class, () -> new MongoTasksRefresher(
                buffer, enhancedSourceCoordinator, pluginMetrics, acknowledgementSetManager,
                executorServiceFunction, null, documentDBSourceAggregateMetrics));
    }
}