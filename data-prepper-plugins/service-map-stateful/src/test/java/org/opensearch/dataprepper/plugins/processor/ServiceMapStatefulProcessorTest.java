/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import com.google.common.collect.Sets;
import io.micrometer.core.instrument.Measurement;
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;

import java.io.File;
import java.lang.reflect.Field;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.processor.ServiceMapProcessorConfig.DEFAULT_WINDOW_DURATION;


public class ServiceMapStatefulProcessorTest {

    private static final String FRONTEND_SERVICE = "FRONTEND";
    private static final String CHECKOUT_SERVICE = "CHECKOUT";
    private static final String AUTHENTICATION_SERVICE = "AUTH";
    private static final String PASSWORD_DATABASE = "PASS";
    private static final String PAYMENT_SERVICE = "PAY";
    private static final String CART_SERVICE = "CART";
    private PluginSetting pluginSetting;
    private PluginMetrics pluginMetrics;
    private PipelineDescription pipelineDescription;
    private ServiceMapProcessorConfig serviceMapProcessorConfig;

    @BeforeEach
    public void setup() throws NoSuchFieldException, IllegalAccessException {
        resetServiceMapStatefulProcessorStatic();
        MetricsTestUtil.initMetrics();
        pluginSetting = mock(PluginSetting.class);
        pipelineDescription = mock(PipelineDescription.class);
        serviceMapProcessorConfig = mock(ServiceMapProcessorConfig.class);
        when(serviceMapProcessorConfig.getWindowDuration()).thenReturn(DEFAULT_WINDOW_DURATION);
        pluginMetrics = PluginMetrics.fromNames(
                "testServiceMapProcessor", "testPipelineName");
        when(pluginSetting.getName()).thenReturn("testServiceMapProcessor");
        when(pluginSetting.getPipelineName()).thenReturn("testPipelineName");
    }

    public void resetServiceMapStatefulProcessorStatic() throws NoSuchFieldException, IllegalAccessException {
        reflectivelySetField(ServiceMapStatefulProcessor.class, "RELATIONSHIP_STATE", Sets.newConcurrentHashSet());
        reflectivelySetField(ServiceMapStatefulProcessor.class, "processorsCreated", new AtomicInteger(0));
        reflectivelySetField(ServiceMapStatefulProcessor.class, "previousTimestamp", 0);
        reflectivelySetField(ServiceMapStatefulProcessor.class, "windowDurationMillis", 0);
        reflectivelySetField(ServiceMapStatefulProcessor.class, "dbPath", null);
        reflectivelySetField(ServiceMapStatefulProcessor.class, "clock", null);
        reflectivelySetField(ServiceMapStatefulProcessor.class, "currentWindow", null);
        reflectivelySetField(ServiceMapStatefulProcessor.class, "previousWindow", null);
        reflectivelySetField(ServiceMapStatefulProcessor.class, "currentTraceGroupWindow", null);
        reflectivelySetField(ServiceMapStatefulProcessor.class, "previousTraceGroupWindow", null);
        reflectivelySetField(ServiceMapStatefulProcessor.class, "allThreadsCyclicBarrier", null);
    }

    private void reflectivelySetField(final Class<?> clazz, final String fieldName, final Object value) throws NoSuchFieldException, IllegalAccessException {
        final Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        final Object fieldValue = field.get(clazz);
        try {
            if (fieldValue instanceof Set) {
                ((Set) fieldValue).clear();
            } else if (fieldValue instanceof AtomicInteger) {
                ((AtomicInteger) fieldValue).set(0);
            } else {
                field.set(clazz, value);
            }
        } finally {
            field.setAccessible(false);
        }
    }

    /**
     * This function mocks what the frontend will do to resolve the data in the service map index to find the edges
     * for the service map.
     *
     * @param serviceMapRelationships List of ServiceMapRelationship objects to be evaluated
     * @return Set of ServiceMapSourceDest objects representing service map edges that were found.
     */
    private Set<ServiceMapSourceDest> evaluateEdges(Set<ServiceMapRelationship> serviceMapRelationships) {
        return serviceMapRelationships.stream()
                .filter(serviceMapRelationship -> serviceMapRelationship.getDestination() != null)
                .map(serviceMapRelationship -> {
                    final String sourceServiceName = serviceMapRelationship.getServiceName();
                    final String destServiceName = serviceMapRelationships.stream()
                            .filter(
                                    otherEdge -> serviceMapRelationship.getDestination().equals(otherEdge.getTarget())
                            ).findFirst().get().getServiceName();
                    return new ServiceMapSourceDest(sourceServiceName, destServiceName);
                }).collect(Collectors.toSet());
    }

    @Test
    public void testDataPrepperConstructor() {
        when(pipelineDescription.getNumberOfProcessWorkers()).thenReturn(4);
        when(serviceMapProcessorConfig.getDbPath()).thenReturn(ServiceMapProcessorConfig.DEFAULT_DB_PATH);
        //Nothing is accessible to validate, so just verify that no exception is thrown.
        final ServiceMapStatefulProcessor serviceMapStatefulProcessor = new ServiceMapStatefulProcessor(
                serviceMapProcessorConfig, pluginMetrics, pipelineDescription);
    }

    @Test
    public void testTraceGroupsWithEventRecordData() throws Exception {
        final Clock clock = Mockito.mock(Clock.class);
        Mockito.when(clock.millis()).thenReturn(1L);
        Mockito.when(clock.instant()).thenReturn(Instant.now());
        ExecutorService threadpool = Executors.newCachedThreadPool();
        final File path = new File(ServiceMapProcessorConfig.DEFAULT_DB_PATH);
        final ServiceMapStatefulProcessor serviceMapStateful1 = new ServiceMapStatefulProcessor(100, path, clock, 2, pluginMetrics);
        final ServiceMapStatefulProcessor serviceMapStateful2 = new ServiceMapStatefulProcessor(100, path, clock, 2, pluginMetrics);

        final byte[] rootSpanId1Bytes = ServiceMapTestUtils.getRandomBytes(8);
        final byte[] rootSpanId2Bytes = ServiceMapTestUtils.getRandomBytes(8);
        final byte[] traceId1Bytes = ServiceMapTestUtils.getRandomBytes(16);
        final byte[] traceId2Bytes = ServiceMapTestUtils.getRandomBytes(16);
        final String rootSpanId1 = Hex.encodeHexString(rootSpanId1Bytes);
        final String rootSpanId2 = Hex.encodeHexString(rootSpanId2Bytes);
        final String traceId1 = Hex.encodeHexString(traceId1Bytes);
        final String traceId2 = Hex.encodeHexString(traceId2Bytes);

        final String traceGroup1 = "reset_password";
        final String traceGroup2 = "checkout";

        final Span frontendSpans1 = ServiceMapTestUtils.getSpan(FRONTEND_SERVICE, traceGroup1, rootSpanId1,
                "", traceId1, SPAN_KIND_CLIENT);
        final Span authenticationSpansServer = ServiceMapTestUtils.getSpan(AUTHENTICATION_SERVICE, "reset",
                Hex.encodeHexString(ServiceMapTestUtils.getRandomBytes(8)), frontendSpans1.getSpanId(), traceId1,
                io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER);
        final Span authenticationSpansClient = ServiceMapTestUtils.getSpan(AUTHENTICATION_SERVICE, "reset",
                Hex.encodeHexString(ServiceMapTestUtils.getRandomBytes(8)), authenticationSpansServer.getSpanId(), traceId1,
                SPAN_KIND_CLIENT);
        final Span passwordDbSpans = ServiceMapTestUtils.getSpan(PASSWORD_DATABASE, "update",
                Hex.encodeHexString(ServiceMapTestUtils.getRandomBytes(8)), authenticationSpansClient.getSpanId(), traceId1,
                io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER);

        final Span frontendSpans2 = ServiceMapTestUtils.getSpan(FRONTEND_SERVICE, traceGroup2, rootSpanId2,
                "", traceId2, SPAN_KIND_CLIENT);
        final Span checkoutSpansServer = ServiceMapTestUtils.getSpan(CHECKOUT_SERVICE, "checkout",
                Hex.encodeHexString(ServiceMapTestUtils.getRandomBytes(8)), rootSpanId2, traceId2,
                io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER);
        final Span checkoutSpansClient = ServiceMapTestUtils.getSpan(CHECKOUT_SERVICE, "checkout",
                Hex.encodeHexString(ServiceMapTestUtils.getRandomBytes(8)), checkoutSpansServer.getSpanId(), traceId2,
                SPAN_KIND_CLIENT);
        final Span cartSpans = ServiceMapTestUtils.getSpan(CART_SERVICE, "get_items",
                Hex.encodeHexString(ServiceMapTestUtils.getRandomBytes(8)), checkoutSpansClient.getSpanId(), traceId2,
                io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER);
        final Span paymentSpans = ServiceMapTestUtils.getSpan(PAYMENT_SERVICE, "charge",
                Hex.encodeHexString(ServiceMapTestUtils.getRandomBytes(8)), checkoutSpansClient.getSpanId(), traceId2,
                io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER);


        //Expected relationships
        final ServiceMapRelationship frontendAuth = ServiceMapRelationship.newDestinationRelationship(
                FRONTEND_SERVICE, SPAN_KIND_CLIENT.name(), AUTHENTICATION_SERVICE, "reset", traceGroup1);
        final ServiceMapRelationship authPassword = ServiceMapRelationship.newDestinationRelationship(
                AUTHENTICATION_SERVICE, SPAN_KIND_CLIENT.name(), PASSWORD_DATABASE, "update", traceGroup1);
        final ServiceMapRelationship frontendCheckout = ServiceMapRelationship.newDestinationRelationship(
                FRONTEND_SERVICE, SPAN_KIND_CLIENT.name(), CHECKOUT_SERVICE, "checkout", traceGroup2);
        final ServiceMapRelationship checkoutCart = ServiceMapRelationship.newDestinationRelationship(
                CHECKOUT_SERVICE, SPAN_KIND_CLIENT.name(), CART_SERVICE, "get_items", traceGroup2);
        final ServiceMapRelationship checkoutPayment = ServiceMapRelationship.newDestinationRelationship(
                CHECKOUT_SERVICE, SPAN_KIND_CLIENT.name(), PAYMENT_SERVICE, "charge", traceGroup2);

        final ServiceMapRelationship checkoutTarget = ServiceMapRelationship.newTargetRelationship(
                CHECKOUT_SERVICE, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER.name(), CHECKOUT_SERVICE, "checkout", traceGroup2);
        final ServiceMapRelationship authTarget = ServiceMapRelationship.newTargetRelationship(
                AUTHENTICATION_SERVICE, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER.name(), AUTHENTICATION_SERVICE, "reset", traceGroup1);
        final ServiceMapRelationship passwordTarget = ServiceMapRelationship.newTargetRelationship(
                PASSWORD_DATABASE, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER.name(), PASSWORD_DATABASE, "update", traceGroup1);
        final ServiceMapRelationship paymentTarget = ServiceMapRelationship.newTargetRelationship(
                PAYMENT_SERVICE, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER.name(), PAYMENT_SERVICE, "charge", traceGroup2);
        final ServiceMapRelationship cartTarget = ServiceMapRelationship.newTargetRelationship(
                CART_SERVICE, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER.name(), CART_SERVICE, "get_items", traceGroup2);

        final Set<ServiceMapRelationship> relationshipsFound = new HashSet<>();

        //First batch
        Mockito.when(clock.millis()).thenReturn(110L);
        Future<Set<ServiceMapRelationship>> r1 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful1,
                Arrays.asList(new Record<>(frontendSpans1), new Record<>(checkoutSpansServer)));
        Future<Set<ServiceMapRelationship>> r2 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful2,
                Arrays.asList(new Record<>(frontendSpans2), new Record<>(checkoutSpansClient)));
        relationshipsFound.addAll(r1.get());
        relationshipsFound.addAll(r2.get());

        //Shouldn't find any relationships
        Assertions.assertEquals(0, relationshipsFound.size());

        //Second batch
        Mockito.when(clock.millis()).thenReturn(220L);
        Future<Set<ServiceMapRelationship>> r3 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful1,
                Arrays.asList(new Record<>(authenticationSpansServer), new Record<>(authenticationSpansClient), new Record<>(cartSpans)));
        Future<Set<ServiceMapRelationship>> r4 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful2,
                Arrays.asList(new Record<>(passwordDbSpans), new Record<>(paymentSpans)));
        relationshipsFound.addAll(r3.get());
        relationshipsFound.addAll(r4.get());

        //Should find the frontend->checkout relationship indicated in the first batch
        Assertions.assertEquals(2, relationshipsFound.size());
        assertTrue(relationshipsFound.containsAll(Arrays.asList(
                frontendCheckout,
                checkoutTarget
        )));

        //Third batch
        Mockito.when(clock.millis()).thenReturn(340L);
        Future<Set<ServiceMapRelationship>> r5 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful1, Arrays.asList());
        Future<Set<ServiceMapRelationship>> r6 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful2, Arrays.asList());
        relationshipsFound.addAll(r5.get());
        relationshipsFound.addAll(r6.get());

        //Should find the rest of the relationships
        Assertions.assertEquals(10, relationshipsFound.size());
        assertTrue(relationshipsFound.containsAll(Arrays.asList(
                frontendAuth,
                authTarget,
                authPassword,
                passwordTarget,
                checkoutCart,
                cartTarget,
                checkoutPayment,
                paymentTarget
        )));

        // Extra validation
        final List<ServiceMapSourceDest> expectedSourceDests = Arrays.asList(
                new ServiceMapSourceDest(FRONTEND_SERVICE, AUTHENTICATION_SERVICE),
                new ServiceMapSourceDest(AUTHENTICATION_SERVICE, PASSWORD_DATABASE),
                new ServiceMapSourceDest(FRONTEND_SERVICE, CHECKOUT_SERVICE),
                new ServiceMapSourceDest(CHECKOUT_SERVICE, CART_SERVICE),
                new ServiceMapSourceDest(CHECKOUT_SERVICE, PAYMENT_SERVICE)
        );

        assertTrue(evaluateEdges(relationshipsFound).containsAll(expectedSourceDests));

        // Verify gauges
        final List<Measurement> spansDbSizeMeasurementList = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add("testPipelineName").add("testServiceMapProcessor")
                        .add(ServiceMapStatefulProcessor.SPANS_DB_SIZE).toString());
        assertThat(spansDbSizeMeasurementList.size(), equalTo(1));

        final List<Measurement> traceGroupDbSizeMeasurementList = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add("testPipelineName").add("testServiceMapProcessor")
                        .add(ServiceMapStatefulProcessor.TRACE_GROUP_DB_SIZE).toString());
        assertThat(traceGroupDbSizeMeasurementList.size(), equalTo(1));

        final List<Measurement> spansDbCountMeasurementList = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add("testPipelineName").add("testServiceMapProcessor")
                        .add(ServiceMapStatefulProcessor.SPANS_DB_COUNT).toString());
        assertThat(spansDbCountMeasurementList.size(), equalTo(1));
        final Measurement spansDbCountMeasurement = spansDbCountMeasurementList.get(0);
        assertThat(spansDbCountMeasurement.getValue(), equalTo(5.0));

        final List<Measurement> traceGroupDbCountMeasurementList = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add("testPipelineName").add("testServiceMapProcessor")
                        .add(ServiceMapStatefulProcessor.TRACE_GROUP_DB_COUNT).toString());
        assertThat(traceGroupDbCountMeasurementList.size(), equalTo(1));
        final Measurement traceGroupDbCountMeasurement = traceGroupDbCountMeasurementList.get(0);
        assertThat(traceGroupDbCountMeasurement.getValue(), equalTo(0.0));

        final List<Measurement> relationshipCountMeasurementList = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add("testPipelineName").add("testServiceMapProcessor")
                        .add(ServiceMapStatefulProcessor.RELATIONSHIP_COUNT).toString());
        assertThat(relationshipCountMeasurementList.size(), equalTo(1));
        final Measurement relationshipCountMeasurement = relationshipCountMeasurementList.get(0);
        assertThat(relationshipCountMeasurement.getValue(), equalTo((double)relationshipsFound.size()));


        final byte[] rootSpanId3Bytes = ServiceMapTestUtils.getRandomBytes(8);
        final byte[] traceId3Bytes = ServiceMapTestUtils.getRandomBytes(16);
        final String rootSpanId3 = Hex.encodeHexString(rootSpanId3Bytes);
        final String traceId3 = Hex.encodeHexString(traceId3Bytes);
        final Span frontendSpans3 = ServiceMapTestUtils.getSpan(
                FRONTEND_SERVICE, traceGroup1, rootSpanId3, rootSpanId3, traceId3, SPAN_KIND_CLIENT);
        final Span authenticationSpansServer2 = ServiceMapTestUtils.getSpan(
                AUTHENTICATION_SERVICE, "reset", Hex.encodeHexString(ServiceMapTestUtils.getRandomBytes(8)),
                frontendSpans3.getSpanId(), traceId3, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER);

        // relationship missing traceGroupName
        when(clock.millis()).thenReturn(450L);
        Future<Set<ServiceMapRelationship>> r7 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful1,
                Collections.singletonList(new Record<>(frontendSpans3)));
        Future<Set<ServiceMapRelationship>> r8 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful2,
                Collections.singletonList(new Record<>(authenticationSpansServer2)));
        final Set<ServiceMapRelationship> relationshipsFoundWithNoTraceGroupName = new HashSet<>();
        relationshipsFoundWithNoTraceGroupName.addAll(r7.get());
        relationshipsFoundWithNoTraceGroupName.addAll(r8.get());

        when(clock.millis()).thenReturn(560L);
        Future<Set<ServiceMapRelationship>> r9 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful1, Arrays.asList());
        Future<Set<ServiceMapRelationship>> r10 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful2, Arrays.asList());
        relationshipsFoundWithNoTraceGroupName.addAll(r9.get());
        relationshipsFoundWithNoTraceGroupName.addAll(r10.get());
        assertThat(relationshipsFoundWithNoTraceGroupName.size(), equalTo(4));
        relationshipsFoundWithNoTraceGroupName.forEach(
                relationship -> assertThat(relationship.getTraceGroupName(), nullValue()));
        serviceMapStateful1.shutdown();
        serviceMapStateful2.shutdown();
    }

    @Test
    public void testTraceGroupsWithIsolatedServiceEventRecordData() throws Exception {
        final Clock clock = Mockito.mock(Clock.class);
        Mockito.when(clock.millis()).thenReturn(1L);
        Mockito.when(clock.instant()).thenReturn(Instant.now());
        ExecutorService threadpool = Executors.newCachedThreadPool();
        final File path = new File(ServiceMapProcessorConfig.DEFAULT_DB_PATH);
        final ServiceMapStatefulProcessor serviceMapStateful1 = new ServiceMapStatefulProcessor(100, path, clock, 2, pluginMetrics);
        final ServiceMapStatefulProcessor serviceMapStateful2 = new ServiceMapStatefulProcessor(100, path, clock, 2, pluginMetrics);

        final byte[] rootSpanIdBytes = ServiceMapTestUtils.getRandomBytes(8);
        final byte[] traceIdBytes = ServiceMapTestUtils.getRandomBytes(16);
        final String rootSpanId = Hex.encodeHexString(rootSpanIdBytes);
        final String traceId = Hex.encodeHexString(traceIdBytes);

        final String traceGroup = "reset_password";

        final Span frontendSpans1 = ServiceMapTestUtils.getSpan(FRONTEND_SERVICE, traceGroup, rootSpanId,
                "", traceId, SPAN_KIND_CLIENT);
        final Span frontendSpans2 = ServiceMapTestUtils.getSpan(FRONTEND_SERVICE, "reset",
                Hex.encodeHexString(ServiceMapTestUtils.getRandomBytes(8)), frontendSpans1.getSpanId(), traceId,
                io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER);

        //Expected relationships
        final ServiceMapRelationship frontend = ServiceMapRelationship.newIsolatedService(
                FRONTEND_SERVICE, traceGroup);

        final Set<ServiceMapRelationship> relationshipsFound = new HashSet<>();

        Future<Set<ServiceMapRelationship>> r1 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful1,
                Arrays.asList(new Record<>(frontendSpans1)));
        Future<Set<ServiceMapRelationship>> r2 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful2,
                Arrays.asList(new Record<>(frontendSpans2)));
        relationshipsFound.addAll(r1.get());
        relationshipsFound.addAll(r2.get());

        Mockito.when(clock.millis()).thenReturn(110L);
        Future<Set<ServiceMapRelationship>> r3 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful1,
                Collections.emptyList());
        Future<Set<ServiceMapRelationship>> r4 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful2,
                Collections.emptyList());
        relationshipsFound.addAll(r3.get());
        relationshipsFound.addAll(r4.get());

        //Shouldn't find any relationships
        Assertions.assertEquals(0, relationshipsFound.size());

        Mockito.when(clock.millis()).thenReturn(220L);
        r3 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful1,
                Collections.emptyList());
        r4 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful2,
                Collections.emptyList());
        relationshipsFound.addAll(r3.get());
        relationshipsFound.addAll(r4.get());

        //Should find the frontend isolated service
        Assertions.assertEquals(1, relationshipsFound.size());
        assertTrue(relationshipsFound.contains(frontend));
    }

    @Test
    public void testPrepareForShutdownWithEventRecordData() {
        final File path = new File(ServiceMapProcessorConfig.DEFAULT_DB_PATH);
        final ServiceMapStatefulProcessor serviceMapStateful = new ServiceMapStatefulProcessor(100, path, Clock.systemUTC(), 1, pluginMetrics);

        final byte[] rootSpanId1Bytes = ServiceMapTestUtils.getRandomBytes(8);
        final byte[] traceId1Bytes = ServiceMapTestUtils.getRandomBytes(16);
        final String rootSpanId1 = Hex.encodeHexString(rootSpanId1Bytes);
        final String traceId1 = Hex.encodeHexString(traceId1Bytes);
        final String traceGroup1 = "reset_password";

        final Span frontendSpans1 = ServiceMapTestUtils.getSpan(
                FRONTEND_SERVICE, traceGroup1, rootSpanId1, "", traceId1, SPAN_KIND_CLIENT);
        final Span authenticationSpansServer = ServiceMapTestUtils.getSpan(
                AUTHENTICATION_SERVICE, "reset", Hex.encodeHexString(ServiceMapTestUtils.getRandomBytes(8)),
                frontendSpans1.getSpanId(), traceId1, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER);

        serviceMapStateful.execute(Arrays.asList(new Record<>(frontendSpans1), new Record<>(authenticationSpansServer)));

        assertFalse(serviceMapStateful.isReadyForShutdown());

        serviceMapStateful.prepareForShutdown();
        serviceMapStateful.execute(Collections.emptyList());

        assertTrue(serviceMapStateful.isReadyForShutdown());

        serviceMapStateful.shutdown();
    }

    @Test
    public void testGetIdentificationKeys() {
        when(pipelineDescription.getNumberOfProcessWorkers()).thenReturn(4);
        when(serviceMapProcessorConfig.getDbPath()).thenReturn(ServiceMapProcessorConfig.DEFAULT_DB_PATH);
        final ServiceMapStatefulProcessor serviceMapStatefulProcessor = new ServiceMapStatefulProcessor(
                serviceMapProcessorConfig, pluginMetrics, pipelineDescription);
        final Collection<String> expectedIdentificationKeys = serviceMapStatefulProcessor.getIdentificationKeys();

        assertThat(expectedIdentificationKeys, equalTo(Collections.singleton("traceId")));
    }

    private static class ServiceMapSourceDest {
        final String source;
        final String dest;

        public ServiceMapSourceDest(final String source, final String dest) {
            this.source = source;
            this.dest = dest;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ServiceMapSourceDest that = (ServiceMapSourceDest) o;
            return Objects.equals(source, that.source) &&
                    Objects.equals(dest, that.dest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(source, dest);
        }
    }
}

