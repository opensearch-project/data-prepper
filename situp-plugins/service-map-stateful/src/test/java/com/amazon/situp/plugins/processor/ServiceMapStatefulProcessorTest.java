package com.amazon.situp.plugins.processor;

import com.amazon.situp.model.record.Record;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;


public class ServiceMapStatefulProcessorTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final String FRONTEND_SERVICE = "FRONTEND";
    private static final String CHECKOUT_SERVICE = "CHECKOUT";
    private static final String AUTHENTICATION_SERVICE = "AUTH";
    private static final String PASSWORD_DATABASE = "PASS";
    private static final String PAYMENT_SERVICE = "PAY";
    private static final String CART_SERVICE = "CART";

    /**
     * This function mocks what the frontend will do to resolve the data in the service map index to find the edges
     * for the service map.
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
    public void testTraceGroups() throws Exception {
        final Clock clock = Mockito.mock(Clock.class);
        Mockito.when(clock.millis()).thenReturn(1L);
        ExecutorService threadpool = Executors.newCachedThreadPool();
        final File path = temporaryFolder.newFolder();
        final ServiceMapStatefulProcessor serviceMapStateful1 = new ServiceMapStatefulProcessor(100, path, clock);
        final ServiceMapStatefulProcessor serviceMapStateful2 = new ServiceMapStatefulProcessor(100, path, clock);

        final byte[] rootSpanId1 = ServiceMapTestUtils.getRandomBytes(8);
        final byte[] rootSpanId2 = ServiceMapTestUtils.getRandomBytes(8);
        final byte[] traceId1 = ServiceMapTestUtils.getRandomBytes(16);
        final byte[] traceId2 = ServiceMapTestUtils.getRandomBytes(16);
        final String traceGroup1 = "reset_password";
        final String traceGroup2 = "checkout";

        final ResourceSpans frontendSpans1 = ServiceMapTestUtils.getResourceSpans(FRONTEND_SERVICE, traceGroup1, rootSpanId1, null, traceId1, Span.SpanKind.CLIENT);
        final ResourceSpans authenticationSpansServer = ServiceMapTestUtils.getResourceSpans(AUTHENTICATION_SERVICE, "reset", ServiceMapTestUtils.getRandomBytes(8), ServiceMapTestUtils.getSpanId(frontendSpans1), traceId1, Span.SpanKind.SERVER);
        final ResourceSpans authenticationSpansClient = ServiceMapTestUtils.getResourceSpans(AUTHENTICATION_SERVICE, "reset", ServiceMapTestUtils.getRandomBytes(8), ServiceMapTestUtils.getSpanId(authenticationSpansServer), traceId1, Span.SpanKind.CLIENT);
        final ResourceSpans passwordDbSpans = ServiceMapTestUtils.getResourceSpans(PASSWORD_DATABASE, "update", ServiceMapTestUtils.getRandomBytes(8), ServiceMapTestUtils.getSpanId(authenticationSpansClient), traceId1, Span.SpanKind.SERVER);

        final ResourceSpans frontendSpans2 = ServiceMapTestUtils.getResourceSpans(FRONTEND_SERVICE, traceGroup2, rootSpanId2, null, traceId2, Span.SpanKind.CLIENT);
        final ResourceSpans checkoutSpansServer = ServiceMapTestUtils.getResourceSpans(CHECKOUT_SERVICE, "checkout", ServiceMapTestUtils.getRandomBytes(8), rootSpanId2, traceId2, Span.SpanKind.SERVER);
        final ResourceSpans checkoutSpansClient = ServiceMapTestUtils.getResourceSpans(CHECKOUT_SERVICE, "checkout", ServiceMapTestUtils.getRandomBytes(8), ServiceMapTestUtils.getSpanId(checkoutSpansServer), traceId2, Span.SpanKind.CLIENT);
        final ResourceSpans cartSpans = ServiceMapTestUtils.getResourceSpans(CART_SERVICE, "get_items", ServiceMapTestUtils.getRandomBytes(8), ServiceMapTestUtils.getSpanId(checkoutSpansClient), traceId2, Span.SpanKind.SERVER);
        final ResourceSpans paymentSpans = ServiceMapTestUtils.getResourceSpans(PAYMENT_SERVICE, "charge", ServiceMapTestUtils.getRandomBytes(8), ServiceMapTestUtils.getSpanId(checkoutSpansClient), traceId2, Span.SpanKind.SERVER);


        //Expected relationships
        final ServiceMapRelationship frontendAuth = ServiceMapRelationship.newDestinationRelationship(FRONTEND_SERVICE, Span.SpanKind.CLIENT.name(), AUTHENTICATION_SERVICE, "reset", traceGroup1);
        final ServiceMapRelationship authPassword = ServiceMapRelationship.newDestinationRelationship(AUTHENTICATION_SERVICE, Span.SpanKind.CLIENT.name(), PASSWORD_DATABASE, "update", traceGroup1);
        final ServiceMapRelationship frontendCheckout = ServiceMapRelationship.newDestinationRelationship(FRONTEND_SERVICE, Span.SpanKind.CLIENT.name(), CHECKOUT_SERVICE, "checkout", traceGroup2);
        final ServiceMapRelationship checkoutCart = ServiceMapRelationship.newDestinationRelationship(CHECKOUT_SERVICE, Span.SpanKind.CLIENT.name(), CART_SERVICE, "get_items", traceGroup2);
        final ServiceMapRelationship checkoutPayment = ServiceMapRelationship.newDestinationRelationship(CHECKOUT_SERVICE, Span.SpanKind.CLIENT.name(), PAYMENT_SERVICE, "charge", traceGroup2);

        final ServiceMapRelationship checkoutTarget = ServiceMapRelationship.newTargetRelationship(CHECKOUT_SERVICE, Span.SpanKind.SERVER.name(), CHECKOUT_SERVICE, "checkout", traceGroup2);
        final ServiceMapRelationship authTarget = ServiceMapRelationship.newTargetRelationship(AUTHENTICATION_SERVICE, Span.SpanKind.SERVER.name(), AUTHENTICATION_SERVICE, "reset", traceGroup1);
        final ServiceMapRelationship passwordTarget = ServiceMapRelationship.newTargetRelationship(PASSWORD_DATABASE, Span.SpanKind.SERVER.name(), PASSWORD_DATABASE, "update", traceGroup1);
        final ServiceMapRelationship paymentTarget = ServiceMapRelationship.newTargetRelationship(PAYMENT_SERVICE, Span.SpanKind.SERVER.name(), PAYMENT_SERVICE, "charge", traceGroup2);
        final ServiceMapRelationship cartTarget = ServiceMapRelationship.newTargetRelationship(CART_SERVICE, Span.SpanKind.SERVER.name(), CART_SERVICE, "get_items", traceGroup2);

        final Set<ServiceMapRelationship> relationshipsFound = new HashSet<>();

        //First batch
        Mockito.when(clock.millis()).thenReturn(110L);
        Future<Set<ServiceMapRelationship>> r1 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful1,
                Collections.singletonList(new Record<>(ServiceMapTestUtils.getExportTraceServiceRequest(frontendSpans1, checkoutSpansServer))));
        Future<Set<ServiceMapRelationship>> r2 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful2,
                Collections.singletonList(new Record<>(ServiceMapTestUtils.getExportTraceServiceRequest(frontendSpans2,checkoutSpansClient))));
        relationshipsFound.addAll(r1.get());
        relationshipsFound.addAll(r2.get());

        //Shouldn't find any relationships
        Assert.assertEquals(0, relationshipsFound.size());

        //Second batch
        Mockito.when(clock.millis()).thenReturn(220L);
        Future<Set<ServiceMapRelationship>> r3 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful1,
                Arrays.asList(new Record<>(ServiceMapTestUtils.getExportTraceServiceRequest(authenticationSpansServer,authenticationSpansClient)),
                        new Record<>(ServiceMapTestUtils.getExportTraceServiceRequest(cartSpans))));
        Future<Set<ServiceMapRelationship>> r4 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful2,
                Collections.singletonList(new Record<>(ServiceMapTestUtils.getExportTraceServiceRequest(passwordDbSpans,paymentSpans))));
        relationshipsFound.addAll(r3.get());
        relationshipsFound.addAll(r4.get());

        //Should find the frontend->checkout relationship indicated in the first batch
        Assert.assertEquals(2, relationshipsFound.size());
        Assert.assertTrue(relationshipsFound.containsAll(Arrays.asList(
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
        Assert.assertTrue(relationshipsFound.containsAll(Arrays.asList(
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

        Assert.assertTrue(evaluateEdges(relationshipsFound).containsAll(expectedSourceDests));

        //Make sure that future relationships that are equivalent are caught by cache
        final byte[] rootSpanId3 = ServiceMapTestUtils.getRandomBytes(8);
        final byte[] traceId3 = ServiceMapTestUtils.getRandomBytes(16);
        final ResourceSpans frontendSpans3 = ServiceMapTestUtils.getResourceSpans(FRONTEND_SERVICE, traceGroup1, rootSpanId3, rootSpanId3, traceId3, Span.SpanKind.CLIENT);
        final ResourceSpans authenticationSpansServer2 = ServiceMapTestUtils.getResourceSpans(AUTHENTICATION_SERVICE, "reset", ServiceMapTestUtils.getRandomBytes(8), ServiceMapTestUtils.getSpanId(frontendSpans3), traceId3, Span.SpanKind.SERVER);

        Mockito.when(clock.millis()).thenReturn(450L);
        Future<Set<ServiceMapRelationship>> r7 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful1,
                Collections.singletonList(new Record<>(ServiceMapTestUtils.getExportTraceServiceRequest(frontendSpans3))));
        Future<Set<ServiceMapRelationship>> r8 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful2,
                Collections.singletonList(new Record<>(ServiceMapTestUtils.getExportTraceServiceRequest(authenticationSpansServer2))));
        Assert.assertTrue(r7.get().isEmpty());
        Assert.assertTrue(r8.get().isEmpty());

        Mockito.when(clock.millis()).thenReturn(560L);
        Future<Set<ServiceMapRelationship>> r9 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful1, Arrays.asList());
        Future<Set<ServiceMapRelationship>> r10 = ServiceMapTestUtils.startExecuteAsync(threadpool, serviceMapStateful2, Arrays.asList());
        Assert.assertTrue(r9.get().isEmpty());
        Assert.assertTrue(r10.get().isEmpty());
        serviceMapStateful1.deleteState();
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

