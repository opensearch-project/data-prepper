package org.opensearch.dataprepper.plugins.kafkaconnect.meter;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;

import static java.util.Collections.emptyList;

public class KafkaConnectMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectMetrics.class);
    private static final String JMX_DOMAIN = "kafka.connect";
    private static final String CONNECT_WORKER_METRICS_NAME = "connect-worker-metrics";
    private static final List<String> CONNECT_WORKER_METRICS_LIST = List.of(
            "task-count",
            "connector-count",
            "connector-startup-attempts-total",
            "connector-startup-success-total",
            "connector-startup-failure-total",
            "task-startup-attempts-total",
            "task-startup-success-total",
            "task-startup-failure-total"
    );
    private static final String SOURCE_TASK_METRICS_NAME = "source-task-metrics";
    private static final List<String> SOURCE_TASK_METRICS_LIST = List.of(
            "source-record-poll-total",
            "source-record-poll-rate",
            "source-record-active-count-max",
            "source-record-active-count-avg",
            "source-record-active-count"
    );

    private final PluginMetrics pluginMetrics;

    private final MBeanServer mBeanServer;

    private final Iterable<Tag> tags;


    public KafkaConnectMetrics(final PluginMetrics pluginMetrics) {
        this(pluginMetrics, emptyList());
    }

    public KafkaConnectMetrics(final PluginMetrics pluginMetrics,
                               final Iterable<Tag> tags) {
        this(pluginMetrics, getMBeanServer(), tags);
    }

    public KafkaConnectMetrics(final PluginMetrics pluginMetrics,
                               final MBeanServer mBeanServer,
                               final Iterable<Tag> tags) {
        this.pluginMetrics = pluginMetrics;
        this.mBeanServer = mBeanServer;
        this.tags = tags;
    }

    private static MBeanServer getMBeanServer() {
        List<MBeanServer> mBeanServers = MBeanServerFactory.findMBeanServer(null);
        if (!mBeanServers.isEmpty()) {
            return mBeanServers.get(0);
        }
        return ManagementFactory.getPlatformMBeanServer();
    }

    private static String sanitize(String value) {
        return value.replaceAll("-", ".");
    }

    public void bindConnectMetrics() {
        registerMetricsEventually(CONNECT_WORKER_METRICS_NAME, (o, tags) -> {
            CONNECT_WORKER_METRICS_LIST.forEach(
                    (metricName) -> registerFunctionGaugeForObject(o, metricName, tags)
            );
            return null;
        });
    }

    public void bindConnectorMetrics() {
        registerMetricsEventually(SOURCE_TASK_METRICS_NAME, (o, tags) -> {
            SOURCE_TASK_METRICS_LIST.forEach(
                    (metricName) -> registerFunctionGaugeForObject(o, metricName, tags)
            );
            return null;
        });
    }

    private void registerMetricsEventually(String type,
                                           BiFunction<ObjectName, Tags, Void> perObject) {
        try {
            Set<ObjectName> objs = mBeanServer.queryNames(new ObjectName(JMX_DOMAIN + ":type=" + type + ",*"), null);
            if (!objs.isEmpty()) {
                for (ObjectName o : objs) {
                    perObject.apply(o, Tags.concat(tags, nameTag(o)));
                }
            }
        } catch (MalformedObjectNameException e) {
            throw new RuntimeException("Error registering Kafka Connect JMX based metrics", e);
        }
    }

    private Iterable<Tag> nameTag(ObjectName name) {
        Tags tags = Tags.empty();

        String clientId = name.getKeyProperty("client-id");
        if (clientId != null) {
            tags = Tags.concat(tags, "client.id", clientId);
        }

        String nodeId = name.getKeyProperty("node-id");
        if (nodeId != null) {
            tags = Tags.concat(tags, "node.id", nodeId);
        }

        String connectorName = name.getKeyProperty("connector");
        if (connectorName != null) {
            tags = Tags.concat(tags, "connector", connectorName);
        }

        String taskName = name.getKeyProperty("task");
        if (taskName != null) {
            tags = Tags.concat(tags, "task", taskName);
        }

        return tags;
    }

    private void registerFunctionGaugeForObject(ObjectName o, String jmxMetricName, Tags allTags) {
        pluginMetrics.gaugeWithTags(
                sanitize(jmxMetricName),
                allTags,
                mBeanServer,
                s -> safeDouble(() -> s.getAttribute(o, jmxMetricName))
        );
    }

    private double safeDouble(Callable<Object> callable) {
        try {
            if (callable.call() == null) return Double.NaN;
            return Double.parseDouble(callable.call().toString());
        } catch (Exception e) {
            return Double.NaN;
        }
    }
}
