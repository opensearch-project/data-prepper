/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.util;

import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.errors.AlreadyExistsException;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfigTransformer;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.distributed.NotLeaderException;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.ConnectRestServer;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.kafkaconnect.extension.WorkerProperties;
import org.opensearch.dataprepper.plugins.kafkaconnect.meter.KafkaConnectMetrics;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaConnectTest {
    private static final String TEST_PIPELINE_NAME = "test";
    private static final WorkerProperties DEFAULT_WORDER_PROPERTY = new WorkerProperties();
    private static final long TEST_CONNECTOR_TIMEOUT_MS = 30000L; // 30 seconds
    private static final long TEST_CONNECT_TIMEOUT_MS = 60000L; // 60 seconds
    private static final Duration TEST_CONNECTOR_TIMEOUT = Duration.ofMillis(TEST_CONNECTOR_TIMEOUT_MS);
    private static final Duration TEST_CONNECT_TIMEOUT = Duration.ofMillis(TEST_CONNECT_TIMEOUT_MS);
    @Mock
    private KafkaConnectMetrics kafkaConnectMetrics;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private DistributedHerder distributedHerder;

    @Mock
    private RestServer rest;

    @Mock
    private Connect connect;


    @BeforeEach
    void setUp() throws Exception {
        kafkaConnectMetrics = mock(KafkaConnectMetrics.class);
        distributedHerder = mock(DistributedHerder.class);
        rest = mock(RestServer.class);
        connect = mock(Connect.class);
        DEFAULT_WORDER_PROPERTY.setBootstrapServers("localhost:9002");

        lenient().when(connect.isRunning()).thenReturn(false).thenReturn(true);
        lenient().when(distributedHerder.connectors()).thenReturn(new ArrayList<>());
        ConnectorStateInfo runningState = new ConnectorStateInfo("newConnector", new ConnectorStateInfo.ConnectorState("RUNNING", "worker", "msg"), new ArrayList<>(), ConnectorType.SOURCE);
        lenient().when(distributedHerder.connectorStatus(any())).thenReturn(runningState);
        lenient().doAnswer(invocation -> {
            Callback<Map<String, String>> callback = invocation.getArgument(1);
            // Simulate a successful completion
            callback.onCompletion(null, null);
            return null;
        }).when(distributedHerder).connectorConfig(any(), any(Callback.class));
        lenient().doAnswer(invocation -> {
            Callback<Herder.Created<ConnectorInfo>> callback = invocation.getArgument(3);
            // Simulate a successful completion
            callback.onCompletion(null, null);
            return null;
        }).when(distributedHerder).putConnectorConfig(any(String.class), any(Map.class), any(Boolean.class), any(Callback.class));
        lenient().doAnswer(invocation -> {
            Callback<Herder.Created<ConnectorInfo>> callback = invocation.getArgument(1);
            // Simulate a successful completion
            callback.onCompletion(null, null);
            return null;
        }).when(distributedHerder).deleteConnectorConfig(any(), any(Callback.class));
    }

    @Test
    void testInitializeKafkaConnectWithSingletonForSamePipeline() {
        final KafkaConnect kafkaConnect = KafkaConnect.getPipelineInstance(TEST_PIPELINE_NAME, pluginMetrics, TEST_CONNECT_TIMEOUT, TEST_CONNECTOR_TIMEOUT);
        final KafkaConnect sameConnect = KafkaConnect.getPipelineInstance(TEST_PIPELINE_NAME, pluginMetrics, TEST_CONNECT_TIMEOUT, TEST_CONNECTOR_TIMEOUT);
        assertThat(sameConnect, is(kafkaConnect));
        final String anotherPipeline = "anotherPipeline";
        final KafkaConnect anotherKafkaConnect = KafkaConnect.getPipelineInstance(anotherPipeline, pluginMetrics, TEST_CONNECT_TIMEOUT, TEST_CONNECTOR_TIMEOUT);
        assertThat(anotherKafkaConnect, not(kafkaConnect));
    }

    @Test
    void testInitializeKafkaConnect() {
        Map<String, String> workerProps = DEFAULT_WORDER_PROPERTY.buildKafkaConnectPropertyMap();
        try (MockedConstruction<DistributedConfig> mockedConfig = mockConstruction(DistributedConfig.class, (mock, context) -> {
            when(mock.kafkaClusterId()).thenReturn("test-cluster-id");
            when(mock.getString(any())).thenReturn("test-string");
        });
             MockedConstruction<RestClient> mockedRestClient = mockConstruction(RestClient.class);
             MockedConstruction<DistributedHerder> mockedHerder = mockConstruction(DistributedHerder.class);
             MockedConstruction<ConnectRestServer> mockedRestServer = mockConstruction(ConnectRestServer.class, (mock, context) -> {
                 when(mock.advertisedUrl()).thenReturn(URI.create("localhost:9002"));
             });
             MockedConstruction<Plugins> mockedPlugin = mockConstruction(Plugins.class, (mock, context) -> {
                 ClassLoader classLoader = mock(ClassLoader.class);
                 ConnectorClientConfigOverridePolicy connectorPolicy = mock(ConnectorClientConfigOverridePolicy.class);
                 when(mock.compareAndSwapWithDelegatingLoader()).thenReturn(classLoader);
                 when(mock.newPlugin(any(), any(), any())).thenReturn(connectorPolicy);
             });
             MockedConstruction<Worker> mockedWorker = mockConstruction(Worker.class, (mock, context) -> {
                 WorkerConfigTransformer configTransformer = mock(WorkerConfigTransformer.class);
                 Converter converter = mock(Converter.class);
                 when(mock.configTransformer()).thenReturn(configTransformer);
                 when(mock.getInternalValueConverter()).thenReturn(converter);
             });
             MockedConstruction<KafkaOffsetBackingStore> mockedOffsetStore = mockConstruction(KafkaOffsetBackingStore.class, (mock, context) -> {
                 doNothing().when(mock).configure(any());
             })
        ) {
            final KafkaConnect kafkaConnect = KafkaConnect.getPipelineInstance(TEST_PIPELINE_NAME, pluginMetrics, TEST_CONNECT_TIMEOUT, TEST_CONNECTOR_TIMEOUT);
            kafkaConnect.initialize(workerProps);
        }
    }

    @Test
    void testStartKafkaConnectSuccess() {
        final KafkaConnect kafkaConnect = new KafkaConnect(distributedHerder, rest, connect, kafkaConnectMetrics);
        doNothing().when(rest).initializeServer();
        doNothing().when(connect).start();
        kafkaConnect.start();
        verify(rest).initializeServer();
        verify(connect).start();
    }

    @Test
    void testStartKafkaConnectFail() {
        final KafkaConnect kafkaConnect = new KafkaConnect(distributedHerder, rest, connect, kafkaConnectMetrics);
        doNothing().when(rest).initializeServer();
        doThrow(new RuntimeException()).when(connect).start();
        doNothing().when(connect).stop();
        assertThrows(RuntimeException.class, kafkaConnect::start);
        verify(connect, times(1)).stop();

        // throw exception immediately if connect is null
        final KafkaConnect kafkaConnect2 = new KafkaConnect(distributedHerder, rest, null, kafkaConnectMetrics);
        assertThrows(RuntimeException.class, kafkaConnect2::start);
    }

    @Test
    void testStartKafkaConnectFailTimeout() {
        doNothing().when(rest).initializeServer();
        doNothing().when(connect).start();
        doNothing().when(connect).stop();
        when(connect.isRunning()).thenReturn(false);
        try (MockedStatic<Clock> mockedStatic = mockStatic(Clock.class)) {
            final Clock clock = mock(Clock.class);
            mockedStatic.when(() -> Clock.systemUTC()).thenReturn(clock);
            when(clock.millis()).thenReturn(0L).thenReturn(TEST_CONNECT_TIMEOUT_MS + 1);
            final KafkaConnect kafkaConnect = new KafkaConnect(distributedHerder, rest, connect, kafkaConnectMetrics);
            assertThrows(RuntimeException.class, kafkaConnect::start);
            verify(rest).initializeServer();
            verify(connect).start();
            verify(connect).stop();
            verify(clock, times(2)).millis();
        }
    }

    @Test
    void testStartKafkaConnectWithConnectRunningAlready() {
        final KafkaConnect kafkaConnect = new KafkaConnect(distributedHerder, rest, connect, kafkaConnectMetrics);
        when(connect.isRunning()).thenReturn(true);
        kafkaConnect.start();
        verify(rest, never()).initializeServer();
        verify(connect, never()).start();
    }

    @Test
    void testStopKafkaConnect() {
        final KafkaConnect kafkaConnect = new KafkaConnect(distributedHerder, rest, connect, kafkaConnectMetrics);
        kafkaConnect.stop();
        verify(connect).stop();
        // should ignore stop if connect is null
        final KafkaConnect kafkaConnect2 = new KafkaConnect(distributedHerder, rest, null, kafkaConnectMetrics);
        kafkaConnect2.stop();
    }

    @Test
    void testInitConnectorsWhenStartKafkaConnectSuccess() {
        final String oldConnectorName = "oldConnector";
        final Connector newConnector = mock(Connector.class);
        final String newConnectorName = "newConnector";
        final Map<String, String> newConnectorConfig = new HashMap<>();
        when(newConnector.getName()).thenReturn(newConnectorName);
        when(newConnector.getConfig()).thenReturn(newConnectorConfig);
        when(distributedHerder.connectors()).thenReturn(List.of(oldConnectorName));

        final KafkaConnect kafkaConnect = new KafkaConnect(distributedHerder, rest, connect, kafkaConnectMetrics);
        kafkaConnect.addConnectors(List.of(newConnector));
        kafkaConnect.start();
        verify(distributedHerder).putConnectorConfig(eq(newConnectorName), eq(newConnectorConfig), eq(true), any(Callback.class));
        verify(distributedHerder).deleteConnectorConfig(eq(oldConnectorName), any(Callback.class));
    }

    @Test
    void testInitConnectorsWithoutConnectorConfigChange() {
        final Connector newConnector = mock(Connector.class);
        final String newConnectorName = "newConnector";
        final Map<String, String> newConnectorConfig = new HashMap<>();
        when(newConnector.getName()).thenReturn(newConnectorName);
        when(newConnector.getConfig()).thenReturn(newConnectorConfig);
        when(newConnector.getAllowReplace()).thenReturn(false);
        doAnswer(invocation -> {
            Callback<Map<String, String>> callback = invocation.getArgument(1);
            // Simulate a successful completion
            callback.onCompletion(null, newConnectorConfig);
            return null;
        }).when(distributedHerder).connectorConfig(any(), any(Callback.class));

        final KafkaConnect kafkaConnect = new KafkaConnect(distributedHerder, rest, connect, kafkaConnectMetrics);
        kafkaConnect.addConnectors(List.of(newConnector));
        kafkaConnect.start();
        verify(distributedHerder).putConnectorConfig(eq(newConnectorName), eq(newConnectorConfig), eq(false), any(Callback.class));
    }

    @Test
    void testInitConnectorsErrorsWhenDeleteConnector() {
        final String oldConnectorName = "oldConnector";
        when(distributedHerder.connectors()).thenReturn(List.of(oldConnectorName));
        final KafkaConnect kafkaConnect = new KafkaConnect(distributedHerder, rest, connect, kafkaConnectMetrics);
        doAnswer(invocation -> {
            Callback<Herder.Created<ConnectorInfo>> callback = invocation.getArgument(1);
            // Simulate a successful completion
            callback.onCompletion(new RuntimeException(), null);
            return null;
        }).when(distributedHerder).deleteConnectorConfig(eq(oldConnectorName), any(Callback.class));
        assertThrows(RuntimeException.class, kafkaConnect::start);
        // NotLeaderException or NotFoundException should be ignored.
        doAnswer(invocation -> {
            Callback<Herder.Created<ConnectorInfo>> callback = invocation.getArgument(1);
            callback.onCompletion(new NotLeaderException("Only Leader can delete.", "leaderUrl"), null);
            return null;
        }).when(distributedHerder).deleteConnectorConfig(eq(oldConnectorName), any(Callback.class));
        kafkaConnect.start();
        doAnswer(invocation -> {
            Callback<Herder.Created<ConnectorInfo>> callback = invocation.getArgument(1);
            // Simulate a successful completion
            callback.onCompletion(new NotFoundException("Not Found"), null);
            return null;
        }).when(distributedHerder).deleteConnectorConfig(eq(oldConnectorName), any(Callback.class));
        kafkaConnect.start();
    }

    @Test
    void testInitConnectorsErrorsWhenPutConnector() {
        final Connector newConnector = mock(Connector.class);
        final String newConnectorName = "newConnector";
        final Map<String, String> newConnectorConfig = new HashMap<>();
        when(newConnector.getName()).thenReturn(newConnectorName);
        when(newConnector.getConfig()).thenReturn(newConnectorConfig);
        final KafkaConnect kafkaConnect = new KafkaConnect(distributedHerder, rest, connect, kafkaConnectMetrics);
        kafkaConnect.addConnectors(List.of(newConnector));
        // RuntimeException should be thrown
        doAnswer(invocation -> {
            Callback<Herder.Created<ConnectorInfo>> callback = invocation.getArgument(3);
            callback.onCompletion(new RuntimeException(), null);
            return null;
        }).when(distributedHerder).putConnectorConfig(eq(newConnectorName), eq(newConnectorConfig), eq(true), any(Callback.class));
        assertThrows(RuntimeException.class, kafkaConnect::start);
        // NotLeaderException or NotFoundException should be ignored.
        doAnswer(invocation -> {
            Callback<Herder.Created<ConnectorInfo>> callback = invocation.getArgument(3);
            callback.onCompletion(new NotLeaderException("not leader", "leaderUrl"), null);
            return null;
        }).when(distributedHerder).putConnectorConfig(eq(newConnectorName), eq(newConnectorConfig), eq(true), any(Callback.class));
        kafkaConnect.start();
        doAnswer(invocation -> {
            Callback<Herder.Created<ConnectorInfo>> callback = invocation.getArgument(3);
            callback.onCompletion(new AlreadyExistsException("Already added"), null);
            return null;
        }).when(distributedHerder).putConnectorConfig(eq(newConnectorName), eq(newConnectorConfig), eq(true), any(Callback.class));
        kafkaConnect.start();
    }

    @Test
    void testInitConnectorsErrorsWhenConnectorsNotRunning() {
        // should throw exception if connector failed in Running state for 30 seconds
        final Connector newConnector = mock(Connector.class);
        final String newConnectorName = "newConnector";
        final Map<String, String> newConnectorConfig = new HashMap<>();
        when(newConnector.getName()).thenReturn(newConnectorName);
        when(newConnector.getConfig()).thenReturn(newConnectorConfig);
        when(distributedHerder.connectorStatus(eq(newConnectorName))).thenReturn(null);

        try (MockedStatic<Clock> mockedStatic = mockStatic(Clock.class)) {
            final Clock clock = mock(Clock.class);
            mockedStatic.when(() -> Clock.systemUTC()).thenReturn(clock);
            when(clock.millis()).thenReturn(0L).thenReturn(0L).thenReturn(0L).thenReturn(0L).thenReturn(TEST_CONNECTOR_TIMEOUT_MS + 1);
            final KafkaConnect kafkaConnect = new KafkaConnect(distributedHerder, rest, connect, kafkaConnectMetrics);
            kafkaConnect.addConnectors(List.of(newConnector));
            assertThrows(RuntimeException.class, kafkaConnect::start);
            verify(distributedHerder, times(1)).connectorStatus(any());
            verify(distributedHerder).putConnectorConfig(eq(newConnectorName), eq(newConnectorConfig), eq(true), any(Callback.class));
            verify(clock, times(5)).millis();
        }
    }
}
