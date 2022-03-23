/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.prepper;

import com.amazon.dataprepper.metrics.MetricNames;
import com.amazon.dataprepper.metrics.MetricsTestUtil;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.trace.Span;
import com.google.common.collect.Sets;
import io.micrometer.core.instrument.Measurement;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.apache.commons.codec.binary.Hex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.lang.reflect.Field;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;


public class ServiceMapStatefulPrepperTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final String FRONTEND_SERVICE = "FRONTEND";
    private static final String CHECKOUT_SERVICE = "CHECKOUT";
    private static final String AUTHENTICATION_SERVICE = "AUTH";
    private static final String PASSWORD_DATABASE = "PASS";
    private static final String PAYMENT_SERVICE = "PAY";
    private static final String CART_SERVICE = "CART";
    private static final PluginSetting PLUGIN_SETTING = new PluginSetting("testServiceMapPrepper", Collections.emptyMap()) {{
        setPipelineName("testPipelineName");
    }};

    @Before
    public void setup() throws NoSuchFieldException, IllegalAccessException {
        resetServiceMapStatefulPrepperStatic();
        MetricsTestUtil.initMetrics();
    }

    public void resetServiceMapStatefulPrepperStatic() throws NoSuchFieldException, IllegalAccessException {
        reflectivelySetField(ServiceMapStatefulPrepper.class, "RELATIONSHIP_STATE", Sets.newConcurrentHashSet());
        reflectivelySetField(ServiceMapStatefulPrepper.class, "preppersCreated", new AtomicInteger(0));
        reflectivelySetField(ServiceMapStatefulPrepper.class, "previousTimestamp", 0);
        reflectivelySetField(ServiceMapStatefulPrepper.class, "windowDurationMillis", 0);
        reflectivelySetField(ServiceMapStatefulPrepper.class, "dbPath", null);
        reflectivelySetField(ServiceMapStatefulPrepper.class, "currentWindow", null);
        reflectivelySetField(ServiceMapStatefulPrepper.class, "previousWindow", null);
        reflectivelySetField(ServiceMapStatefulPrepper.class, "currentTraceGroupWindow", null);
        reflectivelySetField(ServiceMapStatefulPrepper.class, "previousTraceGroupWindow", null);
        reflectivelySetField(ServiceMapStatefulPrepper.class, "allThreadsCyclicBarrier", null);
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
    public void testPluginSettingConstructor() {

        final PluginSetting pluginSetting = new PluginSetting("testPluginSetting", Collections.emptyMap());
        pluginSetting.setProcessWorkers(4);
        pluginSetting.setPipelineName("TestPipeline");
        //Nothing is accessible to validate, so just verify that no exception is thrown.
        final ServiceMapStatefulPrepper serviceMapStatefulPrepper = new ServiceMapStatefulPrepper(pluginSetting);
    }

    // TODO: remove in 2.0
    @Test
    public void testTraceGroupsWithExportTraceServiceRequestRecordData() throws Exception {
        final Clock clock = Mockito.mock(Clock.class);
        Mockito.when(clock.millis()).thenReturn(1L);
        Mockito.when(clock.instant()).thenReturn(Instant.now());
        ExecutorService threadpool = Executors.newCachedThreadPool();
        final File path = new File(ServiceMapPrepperConfig.DEFAULT_DB_PATH);
        final ServiceMapStatefulPrepper serviceMapStateful1 = new ServiceMapStatefulPrepper(100, path, clock, 2, PLUGIN_SETTING);
        final ServiceMapStatefulPrepper serviceMapStateful2 = new ServiceMapStatefulPrepper(100, path, clock, 2, PLUGIN_SETTING);

        final byte[] rootSpanId1 = ServiceMapTestUtils.getRandomBytes(8);
        final byte[] rootSpanId2 = ServiceMapTestUtils.getRandomBytes(8);
        final byte[] traceId1 = ServiceMapTestUtils.getRandomBytes(16);
        final byte[] traceId2 = ServiceMapTestUtils.getRandomBytes(16);
        final String traceGroup1 = "reset_password";
        final String traceGroup2 = "checkout";

        final ResourceSpans frontendSpans1 = ServiceMapTestUtils.getResourceSpans(FRONTEND_SERVICE, traceGroup1, rootSpanId1, null, traceId1, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT);
        final ResourceSpans authenticationSpansServer = ServiceMapTestUtils.getResourceSpans(AUTHENTICATION_SERVICE, "reset", ServiceMapTestUtils.getRandomBytes(8), ServiceMapTestUtils.getSpanId(frontendSpans1), traceId1, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER);
        final ResourceSpans authenticationSpansClient = ServiceMapTestUtils.getResourceSpans(AUTHENTICATION_SERVICE, "reset", ServiceMapTestUtils.getRandomBytes(8), ServiceMapTestUtils.getSpanId(authenticationSpansServer), traceId1, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT);
        final ResourceSpans passwordDbSpans = ServiceMapTestUtils.getResourceSpans(PASSWORD_DATABASE, "update", ServiceMapTestUtils.getRandomBytes(8), ServiceMapTestUtils.getSpanId(authenticationSpansClient), traceId1, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER);

        final ResourceSpans frontendSpans2 = ServiceMapTestUtils.getResourceSpans(FRONTEND_SERVICE, traceGroup2, rootSpanId2, null, traceId2, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT);
        final ResourceSpans checkoutSpansServer = ServiceMapTestUtils.getResourceSpans(CHECKOUT_SERVICE, "checkout", ServiceMapTestUtils.getRandomBytes(8), rootSpanId2, traceId2, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER);
        final ResourceSpans checkoutSpansClient = ServiceMapTestUtils.getResourceSpans(CHECKOUT_SERVICE, "checkout", ServiceMapTestUtils.getRandomBytes(8), ServiceMapTestUtils.getSpanId(checkoutSpansServer), traceId2, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT);
        final ResourceSpans cartSpans = ServiceMapTestUtils.getResourceSpans(CART_SERVICE, "get_items", ServiceMapTestUtils.getRandomBytes(8), ServiceMapTestUtils.getSpanId(checkoutSpansClient), traceId2, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER);
        final ResourceSpans paymentSpans = ServiceMapTestUtils.getResourceSpans(PAYMENT_SERVICE, "charge", ServiceMapTestUtils.getRandomBytes(8), ServiceMapTestUtils.getSpanId(checkoutSpansClient), traceId2, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER);


        //Expected relationships
        final ServiceMapRelationship frontendAuth = ServiceMapRelationship.newDestinationRelationship(FRONTEND_SERVICE, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT.name(), AUTHENTICATION_SERVICE, "reset", traceGroup1);
        final ServiceMapRelationship authPassword = ServiceMapRelationship.newDestinationRelationship(AUTHENTICATION_SERVICE, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT.name(), PASSWORD_DATABASE, "update", traceGroup1);
        final ServiceMapRelationship frontendCheckout = ServiceMapRelationship.newDestinationRelationship(FRONTEND_SERVICE, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT.name(), CHECKOUT_SERVICE, "checkout", traceGroup2);
        final ServiceMapRelationship checkoutCart = ServiceMapRelationship.newDestinationRelationship(CHECKOUT_SERVICE, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT.name(), CART_SERVICE, "get_items", traceGroup2);
        final ServiceMapRelationship checkoutPayment = ServiceMapRelationship.newDestinationRelationship(CHECKOUT_SERVICE, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT.name(), PAYMENT_SERVICE, "charge", traceGroup2);

        final ServiceMapRelationship checkoutTarget = ServiceMapRelationship.newTargetRelationship(CHECKOUT_SERVICE, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER.name(), CHECKOUT_SERVICE, "checkout", traceGroup2);
        final ServiceMapRelationship authTarget = ServiceMapRelationship.newTargetRelationship(AUTHENTICATION_SERVICE, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER.name(), AUTHENTICATION_SERVICE, "reset", traceGroup1);
        final ServiceMapRelationship passwordTarget = ServiceMapRelationship.newTargetRelationship(PASSWORD_DATABASE, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER.name(), PASSWORD_DATABASE, "update", traceGroup1);
        final ServiceMapRelationship paymentTarget = ServiceMapRelationship.newTargetRelationship(PAYMENT_SERVICE, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER.name(), PAYMENT_SERVICE, "charge", traceGroup2);
        final ServiceMapRelationship cartTarget = ServiceMapRelationship.newTargetRelationship(CART_SERVICE, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER.name(), CART_SERVICE, "get_items", traceGroup2);

        final Set<ServiceMapRelationship> relationshipsFound = new HashSet<>();

        //First batch
        Mockito.when(clock.millis()).thenReturn(110L);
        Future<Set<ServiceMapRelationship>> r1 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful1,
                Collections.singletonList(new Record<>(ServiceMapTestUtils.getExportTraceServiceRequest(frontendSpans1, checkoutSpansServer))));
        Future<Set<ServiceMapRelationship>> r2 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful2,
                Collections.singletonList(new Record<>(ServiceMapTestUtils.getExportTraceServiceRequest(frontendSpans2, checkoutSpansClient))));
        relationshipsFound.addAll(r1.get());
        relationshipsFound.addAll(r2.get());

        //Shouldn't find any relationships
        Assert.assertEquals(0, relationshipsFound.size());

        //Second batch
        Mockito.when(clock.millis()).thenReturn(220L);
        Future<Set<ServiceMapRelationship>> r3 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful1,
                Arrays.asList(new Record<>(ServiceMapTestUtils.getExportTraceServiceRequest(authenticationSpansServer, authenticationSpansClient)),
                        new Record<>(ServiceMapTestUtils.getExportTraceServiceRequest(cartSpans))));
        Future<Set<ServiceMapRelationship>> r4 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful2,
                Collections.singletonList(new Record<>(ServiceMapTestUtils.getExportTraceServiceRequest(passwordDbSpans, paymentSpans))));
        relationshipsFound.addAll(r3.get());
        relationshipsFound.addAll(r4.get());

        //Should find the frontend->checkout relationship indicated in the first batch
        Assert.assertEquals(2, relationshipsFound.size());
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
        Assert.assertEquals(10, relationshipsFound.size());
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
        final List<Measurement> spansDbSizeMeasurement = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add("testPipelineName").add("testServiceMapPrepper")
                        .add(ServiceMapStatefulPrepper.SPANS_DB_SIZE).toString());
        Assert.assertEquals(1, spansDbSizeMeasurement.size());

        final List<Measurement> traceGroupDbSizeMeasurement = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add("testPipelineName").add("testServiceMapPrepper")
                        .add(ServiceMapStatefulPrepper.TRACE_GROUP_DB_SIZE).toString());
        Assert.assertEquals(1, traceGroupDbSizeMeasurement.size());


        //Make sure that future relationships that are equivalent are caught by cache
        final byte[] rootSpanId3 = ServiceMapTestUtils.getRandomBytes(8);
        final byte[] traceId3 = ServiceMapTestUtils.getRandomBytes(16);
        final ResourceSpans frontendSpans3 = ServiceMapTestUtils.getResourceSpans(FRONTEND_SERVICE, traceGroup1, rootSpanId3, rootSpanId3, traceId3, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT);
        final ResourceSpans authenticationSpansServer2 = ServiceMapTestUtils.getResourceSpans(AUTHENTICATION_SERVICE, "reset", ServiceMapTestUtils.getRandomBytes(8), ServiceMapTestUtils.getSpanId(frontendSpans3), traceId3, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER);

        when(clock.millis()).thenReturn(450L);
        Future<Set<ServiceMapRelationship>> r7 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful1,
                Collections.singletonList(new Record<>(ServiceMapTestUtils.getExportTraceServiceRequest(frontendSpans3))));
        Future<Set<ServiceMapRelationship>> r8 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful2,
                Collections.singletonList(new Record<>(ServiceMapTestUtils.getExportTraceServiceRequest(authenticationSpansServer2))));
        assertTrue(r7.get().isEmpty());
        assertTrue(r8.get().isEmpty());

        when(clock.millis()).thenReturn(560L);
        Future<Set<ServiceMapRelationship>> r9 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful1, Arrays.asList());
        Future<Set<ServiceMapRelationship>> r10 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful2, Arrays.asList());
        assertTrue(r9.get().isEmpty());
        assertTrue(r10.get().isEmpty());
        serviceMapStateful1.shutdown();
        serviceMapStateful2.shutdown();
    }

    @Test
    public void testTraceGroupsWithEventRecordData() throws Exception {
        final Clock clock = Mockito.mock(Clock.class);
        Mockito.when(clock.millis()).thenReturn(1L);
        Mockito.when(clock.instant()).thenReturn(Instant.now());
        ExecutorService threadpool = Executors.newCachedThreadPool();
        final File path = new File(ServiceMapPrepperConfig.DEFAULT_DB_PATH);
        final ServiceMapStatefulPrepper serviceMapStateful1 = new ServiceMapStatefulPrepper(100, path, clock, 2, PLUGIN_SETTING);
        final ServiceMapStatefulPrepper serviceMapStateful2 = new ServiceMapStatefulPrepper(100, path, clock, 2, PLUGIN_SETTING);

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
                "", traceId1, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT);
        final Span authenticationSpansServer = ServiceMapTestUtils.getSpan(AUTHENTICATION_SERVICE, "reset",
                Hex.encodeHexString(ServiceMapTestUtils.getRandomBytes(8)), frontendSpans1.getSpanId(), traceId1,
                io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER);
        final Span authenticationSpansClient = ServiceMapTestUtils.getSpan(AUTHENTICATION_SERVICE, "reset",
                Hex.encodeHexString(ServiceMapTestUtils.getRandomBytes(8)), authenticationSpansServer.getSpanId(), traceId1,
                io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT);
        final Span passwordDbSpans = ServiceMapTestUtils.getSpan(PASSWORD_DATABASE, "update",
                Hex.encodeHexString(ServiceMapTestUtils.getRandomBytes(8)), authenticationSpansClient.getSpanId(), traceId1,
                io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER);

        final Span frontendSpans2 = ServiceMapTestUtils.getSpan(FRONTEND_SERVICE, traceGroup2, rootSpanId2,
                "", traceId2, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT);
        final Span checkoutSpansServer = ServiceMapTestUtils.getSpan(CHECKOUT_SERVICE, "checkout",
                Hex.encodeHexString(ServiceMapTestUtils.getRandomBytes(8)), rootSpanId2, traceId2,
                io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER);
        final Span checkoutSpansClient = ServiceMapTestUtils.getSpan(CHECKOUT_SERVICE, "checkout",
                Hex.encodeHexString(ServiceMapTestUtils.getRandomBytes(8)), checkoutSpansServer.getSpanId(), traceId2,
                io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT);
        final Span cartSpans = ServiceMapTestUtils.getSpan(CART_SERVICE, "get_items",
                Hex.encodeHexString(ServiceMapTestUtils.getRandomBytes(8)), checkoutSpansClient.getSpanId(), traceId2,
                io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER);
        final Span paymentSpans = ServiceMapTestUtils.getSpan(PAYMENT_SERVICE, "charge",
                Hex.encodeHexString(ServiceMapTestUtils.getRandomBytes(8)), checkoutSpansClient.getSpanId(), traceId2,
                io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER);


        //Expected relationships
        final ServiceMapRelationship frontendAuth = ServiceMapRelationship.newDestinationRelationship(
                FRONTEND_SERVICE, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT.name(), AUTHENTICATION_SERVICE, "reset", traceGroup1);
        final ServiceMapRelationship authPassword = ServiceMapRelationship.newDestinationRelationship(
                AUTHENTICATION_SERVICE, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT.name(), PASSWORD_DATABASE, "update", traceGroup1);
        final ServiceMapRelationship frontendCheckout = ServiceMapRelationship.newDestinationRelationship(
                FRONTEND_SERVICE, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT.name(), CHECKOUT_SERVICE, "checkout", traceGroup2);
        final ServiceMapRelationship checkoutCart = ServiceMapRelationship.newDestinationRelationship(
                CHECKOUT_SERVICE, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT.name(), CART_SERVICE, "get_items", traceGroup2);
        final ServiceMapRelationship checkoutPayment = ServiceMapRelationship.newDestinationRelationship(
                CHECKOUT_SERVICE, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT.name(), PAYMENT_SERVICE, "charge", traceGroup2);

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
        Assert.assertEquals(0, relationshipsFound.size());

        //Second batch
        Mockito.when(clock.millis()).thenReturn(220L);
        Future<Set<ServiceMapRelationship>> r3 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful1,
                Arrays.asList(new Record<>(authenticationSpansServer), new Record<>(authenticationSpansClient), new Record<>(cartSpans)));
        Future<Set<ServiceMapRelationship>> r4 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful2,
                Arrays.asList(new Record<>(passwordDbSpans), new Record<>(paymentSpans)));
        relationshipsFound.addAll(r3.get());
        relationshipsFound.addAll(r4.get());

        //Should find the frontend->checkout relationship indicated in the first batch
        Assert.assertEquals(2, relationshipsFound.size());
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
        Assert.assertEquals(10, relationshipsFound.size());
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
        final List<Measurement> spansDbSizeMeasurement = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add("testPipelineName").add("testServiceMapPrepper")
                        .add(ServiceMapStatefulPrepper.SPANS_DB_SIZE).toString());
        Assert.assertEquals(1, spansDbSizeMeasurement.size());

        final List<Measurement> traceGroupDbSizeMeasurement = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add("testPipelineName").add("testServiceMapPrepper")
                        .add(ServiceMapStatefulPrepper.TRACE_GROUP_DB_SIZE).toString());
        Assert.assertEquals(1, traceGroupDbSizeMeasurement.size());


        //Make sure that future relationships that are equivalent are caught by cache
        final byte[] rootSpanId3Bytes = ServiceMapTestUtils.getRandomBytes(8);
        final byte[] traceId3Bytes = ServiceMapTestUtils.getRandomBytes(16);
        final String rootSpanId3 = Hex.encodeHexString(rootSpanId3Bytes);
        final String traceId3 = Hex.encodeHexString(traceId3Bytes);
        final Span frontendSpans3 = ServiceMapTestUtils.getSpan(
                FRONTEND_SERVICE, traceGroup1, rootSpanId3, rootSpanId3, traceId3, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT);
        final Span authenticationSpansServer2 = ServiceMapTestUtils.getSpan(
                AUTHENTICATION_SERVICE, "reset", Hex.encodeHexString(ServiceMapTestUtils.getRandomBytes(8)),
                frontendSpans3.getSpanId(), traceId3, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER);

        when(clock.millis()).thenReturn(450L);
        Future<Set<ServiceMapRelationship>> r7 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful1,
                Collections.singletonList(new Record<>(frontendSpans3)));
        Future<Set<ServiceMapRelationship>> r8 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful2,
                Collections.singletonList(new Record<>(authenticationSpansServer2)));
        assertTrue(r7.get().isEmpty());
        assertTrue(r8.get().isEmpty());

        when(clock.millis()).thenReturn(560L);
        Future<Set<ServiceMapRelationship>> r9 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful1, Arrays.asList());
        Future<Set<ServiceMapRelationship>> r10 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful2, Arrays.asList());
        assertTrue(r9.get().isEmpty());
        assertTrue(r10.get().isEmpty());
        serviceMapStateful1.shutdown();
        serviceMapStateful2.shutdown();
    }

    // TODO: remove in 2.0
    @Test
    public void testPrepareForShutdownWithExportTraceServiceRequestRecordData() throws Exception {
        final File path = new File(ServiceMapPrepperConfig.DEFAULT_DB_PATH);
        final ServiceMapStatefulPrepper serviceMapStateful = new ServiceMapStatefulPrepper(100, path, Clock.systemUTC(), 1, PLUGIN_SETTING);

        final byte[] rootSpanId1 = ServiceMapTestUtils.getRandomBytes(8);
        final byte[] traceId1 = ServiceMapTestUtils.getRandomBytes(16);
        final String traceGroup1 = "reset_password";

        final ResourceSpans frontendSpans1 = ServiceMapTestUtils.getResourceSpans(FRONTEND_SERVICE, traceGroup1, rootSpanId1, null, traceId1, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT);
        final ResourceSpans authenticationSpansServer = ServiceMapTestUtils.getResourceSpans(AUTHENTICATION_SERVICE, "reset", ServiceMapTestUtils.getRandomBytes(8), ServiceMapTestUtils.getSpanId(frontendSpans1), traceId1, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_SERVER);

        serviceMapStateful.execute(Collections.singletonList(new Record<>(ServiceMapTestUtils.getExportTraceServiceRequest(frontendSpans1, authenticationSpansServer))));

        assertFalse(serviceMapStateful.isReadyForShutdown());

        serviceMapStateful.prepareForShutdown();
        serviceMapStateful.execute(Collections.emptyList());

        assertTrue(serviceMapStateful.isReadyForShutdown());

        serviceMapStateful.shutdown();
    }

    @Test
    public void testPrepareForShutdownWithEventRecordData() {
        final File path = new File(ServiceMapPrepperConfig.DEFAULT_DB_PATH);
        final ServiceMapStatefulPrepper serviceMapStateful = new ServiceMapStatefulPrepper(100, path, Clock.systemUTC(), 1, PLUGIN_SETTING);

        final byte[] rootSpanId1Bytes = ServiceMapTestUtils.getRandomBytes(8);
        final byte[] traceId1Bytes = ServiceMapTestUtils.getRandomBytes(16);
        final String rootSpanId1 = Hex.encodeHexString(rootSpanId1Bytes);
        final String traceId1 = Hex.encodeHexString(traceId1Bytes);
        final String traceGroup1 = "reset_password";

        final Span frontendSpans1 = ServiceMapTestUtils.getSpan(
                FRONTEND_SERVICE, traceGroup1, rootSpanId1, "", traceId1, io.opentelemetry.proto.trace.v1.Span.SpanKind.SPAN_KIND_CLIENT);
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

