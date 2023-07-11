package org.opensearch.dataprepper.plugins.kafkaconnect.meter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaConnectMetricsTest {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private MBeanServer mBeanServer;

    private Iterable tags = emptyList();

    @BeforeEach
    void setUp() throws Exception {
        pluginMetrics = mock(PluginMetrics.class);
        mBeanServer = mock(MBeanServer.class);
        lenient().when(mBeanServer.getAttribute(any(), any())).thenReturn(1);
    }

    @Test
    void testConstructor() {
        assertThat(new KafkaConnectMetrics(pluginMetrics), notNullValue());
        when(mBeanServer.queryNames(any(), any())).thenReturn(emptySet());
        assertThat(new KafkaConnectMetrics(pluginMetrics, tags), notNullValue());
    }

    @Test
    void testBindConnectMetrics() throws MalformedObjectNameException {
        final KafkaConnectMetrics kafkaConnectMetrics = new KafkaConnectMetrics(pluginMetrics, mBeanServer, tags);
        when(mBeanServer.queryNames(any(), any())).thenReturn(Set.of(new ObjectName("test:*")));
        kafkaConnectMetrics.bindConnectMetrics();
        verify(mBeanServer).queryNames(any(), any());
        verify(pluginMetrics, atLeastOnce()).gaugeWithTags(any(), any(), any(), any());
    }

    @Test
    void testBindConnectorMetrics() throws MalformedObjectNameException {
        final KafkaConnectMetrics kafkaConnectMetrics = new KafkaConnectMetrics(pluginMetrics, mBeanServer, tags);
        when(mBeanServer.queryNames(any(), any())).thenReturn(Set.of(new ObjectName("test:type=test,connector=test,client-id=test1,node-id=test1,task=task1")));
        kafkaConnectMetrics.bindConnectorMetrics();
        verify(mBeanServer).queryNames(any(), any());
        verify(pluginMetrics, atLeastOnce()).gaugeWithTags(any(), any(), any(), any());
    }
}
