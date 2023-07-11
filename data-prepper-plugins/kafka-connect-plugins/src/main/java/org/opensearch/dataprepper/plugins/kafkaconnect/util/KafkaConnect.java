/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafkaconnect.util;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.errors.AlreadyExistsException;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.WorkerConfigTransformer;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.distributed.NotLeaderException;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.KafkaConfigBackingStore;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.KafkaStatusBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.SharedTopicAdmin;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.kafkaconnect.meter.KafkaConnectMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;

/**
 * The KafkaConnect infra.
 * Unique with single instance for each pipeline.
 */
public class KafkaConnect {
    public static final long CONNECTOR_TIMEOUT_MS = 30000L; // 30 seconds
    public static final long CONNECT_TIMEOUT_MS = 60000L; // 60 seconds
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConnect.class);
    private static volatile Map<String, KafkaConnect> instanceMap = new HashMap<>();
    private static final long RETRY_INTERVAL_MS = 3000L; // 3 seconds
    private static final int LATCH_WAIT_TIME = 1; // 1 minute
    private final Map<String, Connector> connectorMap;
    private final KafkaConnectMetrics kafkaConnectMetrics;
    private final Time time = Time.SYSTEM;
    private final Clock clock = Clock.systemUTC();
    private DistributedHerder herder;
    private RestServer rest;
    private Connect connect;

    private KafkaConnect(final PluginMetrics pluginMetrics) {
        this.connectorMap = new HashMap<>();
        this.kafkaConnectMetrics = new KafkaConnectMetrics(pluginMetrics);
    }

    /**
     * For Testing
     */
    public KafkaConnect(final DistributedHerder herder,
                        final RestServer rest,
                        final Connect connect,
                        final KafkaConnectMetrics kafkaConnectMetrics) {
        this.connectorMap = new HashMap<>();
        this.herder = herder;
        this.rest = rest;
        this.connect = connect;
        this.kafkaConnectMetrics = kafkaConnectMetrics;
    }

    public static KafkaConnect getPipelineInstance(final String pipelineName,
                                                   final PluginMetrics pluginMetrics) {
        KafkaConnect instance = instanceMap.get(pipelineName);
        if (instance == null) {
            synchronized (KafkaConnect.class) {
                instance = new KafkaConnect(pluginMetrics);
                instanceMap.put(pipelineName, instance);
            }
        }
        return instance;
    }

    public synchronized void initialize(Map<String, String> workerProps) {
        DistributedConfig config = new DistributedConfig(workerProps);
        RestClient restClient = new RestClient(config);
        this.rest = new RestServer(config, restClient);
        this.herder = initHerder(workerProps, config, restClient);
        this.connect = new Connect(herder, rest);
    }

    /**
     * Add connectors to the Kafka Connect.
     * This must be done before the start() is called.
     *
     * @param connectors connectors to be added.
     */
    public void addConnectors(List<Connector> connectors) {
        connectors.forEach(connector -> {
            this.connectorMap.put(connector.getName(), connector);
        });
    }

    /**
     * Start the kafka connect.
     * Will add all connectors, and cleanup unused connectors at once.
     */
    public synchronized void start() {
        if (this.connect == null) {
            throw new RuntimeException("Please initialize Kafka Connect first.");
        }
        if (this.connect.isRunning()) {
            LOG.info("Kafka Connect is running, will not start again");
            return;
        }
        LOG.info("Starting Kafka Connect");
        try {
            this.rest.initializeServer();
            this.connect.start();
            waitForConnectRunning();
            this.kafkaConnectMetrics.bindConnectMetrics();
            this.initConnectors();
        } catch (Exception e) {
            LOG.error("Failed to start Connect", e);
            this.connect.stop();
            throw new RuntimeException(e);
        }
    }

    /**
     * Stop the Kafka Connect.
     */
    public void stop() {
        if (this.connect == null) {
            LOG.info("Kafka Connect is running, will not start again");
            return;
        }
        LOG.info("Stopping Kafka Connect");
        this.connect.stop();
    }

    private DistributedHerder initHerder(Map<String, String> workerProps, DistributedConfig config, RestClient restClient) {
        LOG.info("Scanning for plugin classes. This might take a moment ...");
        Plugins plugins = new Plugins(workerProps);
        plugins.compareAndSwapWithDelegatingLoader();
        String kafkaClusterId = config.kafkaClusterId();
        LOG.info("Kafka cluster ID: {}", kafkaClusterId);

        URI advertisedUrl = rest.advertisedUrl();
        String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();

        String clientIdBase = ConnectUtils.clientIdBase(config);

        // Create the admin client to be shared by all backing stores.
        Map<String, Object> adminProps = new HashMap<>(config.originals());
        ConnectUtils.addMetricsContextProperties(adminProps, config, kafkaClusterId);
        adminProps.put(CLIENT_ID_CONFIG, clientIdBase + "shared-admin");
        SharedTopicAdmin sharedAdmin = new SharedTopicAdmin(adminProps);

        KafkaOffsetBackingStore offsetBackingStore = new KafkaOffsetBackingStore(sharedAdmin, () -> clientIdBase);
        offsetBackingStore.configure(config);

        ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy = plugins.newPlugin(
                config.getString(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG),
                config, ConnectorClientConfigOverridePolicy.class);

        Worker worker = new Worker(workerId, time, plugins, config, offsetBackingStore, connectorClientConfigOverridePolicy);
        WorkerConfigTransformer configTransformer = worker.configTransformer();

        Converter internalValueConverter = worker.getInternalValueConverter();
        StatusBackingStore statusBackingStore = new KafkaStatusBackingStore(time, internalValueConverter, sharedAdmin, clientIdBase);
        statusBackingStore.configure(config);

        ConfigBackingStore configBackingStore = new KafkaConfigBackingStore(
                internalValueConverter,
                config,
                configTransformer,
                sharedAdmin,
                clientIdBase);

        // Pass the shared admin to the distributed herder as an additional AutoCloseable object that should be closed when the
        // herder is stopped. This is easier than having to track and own the lifecycle ourselves.
        return new DistributedHerder(config, time, worker,
                kafkaClusterId, statusBackingStore, configBackingStore,
                advertisedUrl.toString(), restClient, connectorClientConfigOverridePolicy, sharedAdmin);
    }

    /**
     *
     * @throws InterruptedException
     */
    private void waitForConnectRunning() throws InterruptedException {
        long startTime = clock.millis();
        boolean isRunning = false;
        while (clock.millis() - startTime < CONNECT_TIMEOUT_MS) {
            LOG.info("Waiting Kafka Connect running");
            isRunning = this.connect.isRunning();
            if (isRunning) break;
            TimeUnit.MILLISECONDS.sleep(RETRY_INTERVAL_MS);
        }
        if (!isRunning) {
            throw new RuntimeException("Timed out waiting for Kafka Connect running");
        }
    }

    /**
     * Initialize connectors.
     * Will add connectors, and delete undeclared connectors.
     */
    private void initConnectors() throws InterruptedException {
        this.deleteConnectors();
        this.registerConnectors();
        this.waitForConnectorsRunning();
        this.kafkaConnectMetrics.bindConnectorMetrics();
    }

    /**
     * Register Connector to Kafka Connect.
     * Designed as private method to prevent register the connector after connect is started.
     */
    private void registerConnectors() throws InterruptedException {
        CountDownLatch connectorLatch = new CountDownLatch(connectorMap.size());
        List<String> exceptionMessages = new ArrayList<>();
        connectorMap.forEach((connectorName, connector) -> {
            herder.putConnectorConfig(connectorName, connector.getConfig(), connector.getAllowReplace(), (error, result) -> {
                if (error != null) {
                    if (error instanceof NotLeaderException || error instanceof AlreadyExistsException) {
                        LOG.info(error.getMessage());
                    } else {
                        LOG.error("Failed to put connector config: {}", connectorName);
                        exceptionMessages.add(error.getMessage());
                    }
                } else {
                    // Handle the successful registration
                    LOG.info("Success put connector config: {}", connectorName);
                }
                connectorLatch.countDown();
            });
        });
        // Block and wait for all tasks to complete
        if (!connectorLatch.await(LATCH_WAIT_TIME, TimeUnit.MINUTES)) {
            throw new RuntimeException("Timed out waiting for initConnectors");
        } else {
            if (!exceptionMessages.isEmpty()) {
                throw new RuntimeException(String.join(", ", exceptionMessages));
            }
            LOG.info("InitConnectors completed");
        }
    }

    /**
     * Delete Connectors from Kafka Connect.
     * Designed as private method to prevent delete the connector after connect is started.
     */
    private void deleteConnectors() throws InterruptedException {
        Collection<String> connectorsToDelete = this.herder.connectors()
                .stream()
                .filter(connectorName -> !connectorMap.containsKey(connectorName))
                .collect(Collectors.toList());
        List<String> exceptionMessages = new ArrayList<>();
        CountDownLatch deleteLatch = new CountDownLatch(connectorsToDelete.size());
        connectorsToDelete.forEach(connectorName -> {
            herder.deleteConnectorConfig(connectorName, (error, result) -> {
                if (error != null) {
                    if (error instanceof NotLeaderException || error instanceof NotFoundException) {
                        LOG.info(error.getMessage());
                    } else {
                        LOG.error("Failed to delete connector config: {}", connectorName);
                        exceptionMessages.add(error.getMessage());
                    }
                } else {
                    // Handle the successful registration
                    LOG.info("Success delete connector config: {}", connectorName);
                }
                deleteLatch.countDown();
            });
        });
        // Block and wait for all tasks to complete
        if (!deleteLatch.await(LATCH_WAIT_TIME, TimeUnit.MINUTES)) {
            throw new RuntimeException("Timed out waiting for deleteConnectors");
        } else {
            if (!exceptionMessages.isEmpty()) {
                throw new RuntimeException(String.join(", ", exceptionMessages));
            }
            LOG.info("deleteConnectors completed");
        }
    }

    private void waitForConnectorsRunning() throws InterruptedException {
        LOG.info("Waiting for connectors to be running");
        Set<String> connectorNames = this.connectorMap.keySet();
        List<String> exceptionMessages = new ArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(connectorNames.size());
        connectorNames.parallelStream().forEach(connectorName -> {
            long startTime = clock.millis();
            boolean isRunning = false;
            while (clock.millis() - startTime < CONNECTOR_TIMEOUT_MS) {
                try {
                    ConnectorStateInfo info = herder.connectorStatus(connectorName);
                    if ("RUNNING".equals(info.connector().state())) {
                        // Connector is running, decrement the latch count
                        isRunning = true;
                        break;
                    }
                } catch (Exception e) {
                    LOG.info(e.getMessage());
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(RETRY_INTERVAL_MS);
                } catch (InterruptedException e) {
                    break;
                }
            }
            countDownLatch.countDown();
            if (!isRunning) {
                exceptionMessages.add(String.format("Connector %s is not running in desired period of time", connectorName));
            }
        });
        // Block and wait for all tasks to complete
        if (!countDownLatch.await(LATCH_WAIT_TIME, TimeUnit.MINUTES)) {
            throw new RuntimeException("Timed out waiting for running state check");
        } else {
            if (!exceptionMessages.isEmpty()) {
                throw new RuntimeException(String.join(", ", exceptionMessages));
            }
            LOG.info("All connectors are running");
        }
    }
}
