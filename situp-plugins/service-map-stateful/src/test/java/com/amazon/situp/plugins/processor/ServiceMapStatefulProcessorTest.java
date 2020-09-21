package com.amazon.situp.plugins.processor;

import com.google.protobuf.ByteString;
import com.amazon.situp.model.record.Record;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;


public class ServiceMapStatefulProcessorTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String FRONTEND_SERVICE = "FRONTEND";
    private static final String BACKEND_SERVICE = "BACKEND";
    private static final String DATABASE_SERVICE = "DATABASE";
    private static final String CHECKOUT_SERVICE = "CHECKOUT";
    private static final String OTHER_SERVICE = "OTHER";

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

    /**
     * Creates a ResourceSpans object with the given parameters, with a single span
     * @param serviceName Resource name for the ResourceSpans object
     * @param spanName Span name for the single span in the ResourceSpans object
     * @param spanId Span id for the single span in the ResourceSpans object
     * @param parentId Parent id for the single span in the ResourceSpans object
     * @param spanKind Span kind for the single span in the ResourceSpans object
     * @return ResourceSpans object with a single span constructed according to the parameters
     * @throws UnsupportedEncodingException
     */
    public ResourceSpans getResourceSpans(final String serviceName, final String spanName, final byte[]
            spanId, final byte[] parentId, final Span.SpanKind spanKind) throws UnsupportedEncodingException {
        return ResourceSpans.newBuilder()
                .setResource(
                        Resource.newBuilder()
                                .addAttributes(KeyValue.newBuilder()
                                        .setKey("resource.name")
                                        .setValue(AnyValue.newBuilder().setStringValue(serviceName).build()).build())
                                .build()
                )
                .addInstrumentationLibrarySpans(
                        0,
                        InstrumentationLibrarySpans.newBuilder()
                                .addSpans(
                                        Span.newBuilder()
                                                .setName(spanName)
                                                .setKind(spanKind)
                                                .setSpanId(ByteString.copyFrom(spanId))
                                                .setParentSpanId(ByteString.copyFrom(parentId))
                                                .build()
                                )
                                .build()
                )
                .build();
    }

    private static final byte[] SPAN_ID_1 = new byte[]{-120, 0, -120, 0, -120, 0, -120, 0};
    private static final byte[] SPAN_ID_2 = new byte[]{-50, 0, -50, 0, -50, 0, -50, 0};
    private static final byte[] SPAN_ID_3 = new byte[]{-10, 0, -10, 0, -10, 0, -10, 0};
    private static final byte[] SPAN_ID_4 = new byte[]{10, 10, 10, 10, 10, 10, 10, 10};
    private static final byte[] SPAN_ID_5 = new byte[]{20, 20, 20, 20, 20, 20, 20, 20};
    private static final byte[] SPAN_ID_6 = new byte[]{30, 30, 30, 30, 30, 30, 30, 30};
    private static final byte[] SPAN_ID_7 = new byte[]{40, 40, 40, 40, 40, 40, 40, 40};


    /**
     * Test with the following fake trace setup
     *
     * |----  Service = frontend ------------------------------------------------|
     *   |--  Service = backend --------------------------------------------|
     *     |-- Service = database --|    |-- Service = checkout---|
     *
     *                                                                                    |--Service = other--|
     */
    @Test
    public void testServiceMapProcessor()  throws Exception {
        final Clock clock = Mockito.mock(Clock.class);
        Mockito.when(clock.millis()).thenReturn(1L);
        ExecutorService threadpool = Executors.newCachedThreadPool();
        final File path = temporaryFolder.newFolder();
        //This processor will iterate over the front half of key range
        final ServiceMapStatefulProcessor serviceMapStateful1 = new ServiceMapStatefulProcessor(100, path, 2, clock);
        //This processor will iterate over back half of the key range
        final ServiceMapStatefulProcessor serviceMapStateful2 = new ServiceMapStatefulProcessor(100, path, 2, clock);

        //Frontend service client span
        ResourceSpans spans1 = getResourceSpans(FRONTEND_SERVICE, "span1", SPAN_ID_1, SPAN_ID_1, Span.SpanKind.CLIENT);
        //Backend service server and client spans
        ResourceSpans spans2 = getResourceSpans(BACKEND_SERVICE, "span2", SPAN_ID_2, SPAN_ID_1, Span.SpanKind.SERVER);
        ResourceSpans spans3 = getResourceSpans(BACKEND_SERVICE, "span3", SPAN_ID_3, SPAN_ID_2, Span.SpanKind.CLIENT);
        //Database service server span
        ResourceSpans spans4 = getResourceSpans(DATABASE_SERVICE, "span4" ,SPAN_ID_4,SPAN_ID_3, Span.SpanKind.SERVER);
        //Checkoue service server span
        ResourceSpans spans5 = getResourceSpans(CHECKOUT_SERVICE, "span5",SPAN_ID_5, SPAN_ID_3, Span.SpanKind.SERVER);
        //Other service span
        ResourceSpans spans6 = getResourceSpans(OTHER_SERVICE, "span6",SPAN_ID_6,SPAN_ID_7, Span.SpanKind.SPAN_KIND_UNSPECIFIED);

        final ServiceMapRelationship relationship1 = ServiceMapRelationship.newDestinationRelationship(FRONTEND_SERVICE, Span.SpanKind.CLIENT.name(), BACKEND_SERVICE);
        final ServiceMapRelationship relationship2 = ServiceMapRelationship.newDestinationRelationship(BACKEND_SERVICE, Span.SpanKind.CLIENT.name(), DATABASE_SERVICE);
        final ServiceMapRelationship relationship3 = ServiceMapRelationship.newDestinationRelationship(BACKEND_SERVICE, Span.SpanKind.CLIENT.name(), CHECKOUT_SERVICE);
        final ServiceMapRelationship targetRelationship1 = ServiceMapRelationship.newTargetRelationship(BACKEND_SERVICE, Span.SpanKind.SERVER.name(), BACKEND_SERVICE);
        final ServiceMapRelationship targetRelationship2 = ServiceMapRelationship.newTargetRelationship(DATABASE_SERVICE, Span.SpanKind.SERVER.name(), DATABASE_SERVICE);
        final ServiceMapRelationship targetRelationship3 = ServiceMapRelationship.newTargetRelationship(CHECKOUT_SERVICE, Span.SpanKind.SERVER.name(), CHECKOUT_SERVICE);

        //Set clock to close out original window
        Mockito.when(clock.millis()).thenReturn(110L);

        //Process Span 1, should result in no relationships found
        Future<Collection<Record<String>>> emptyFuture1 = threadpool.submit(() -> {
            return serviceMapStateful1.execute(Arrays.asList(
                    new Record<>(spans1)
            ));
        });
        Future<Collection<Record<String>>> emptyFuture2 = threadpool.submit(() -> {
            return serviceMapStateful2.execute(Collections.emptyList());
        });

        Assert.assertTrue(emptyFuture1.get().isEmpty());
        Assert.assertTrue(emptyFuture2.get().isEmpty());

        //Set clock to move on to the next window
        Mockito.when(clock.millis()).thenReturn(220L);


        emptyFuture1 = threadpool.submit(() -> {
            return serviceMapStateful2.execute(Arrays.asList(
                    new Record<>(spans2),
                    new Record<>(spans4)
            ));
        });
        emptyFuture2 = threadpool.submit(()-> {
            return serviceMapStateful1.execute(Arrays.asList(
                    new Record<>(spans3),
                    new Record<>(spans5),
                    new Record<>(spans6)));
        });
        Assert.assertTrue(emptyFuture1.get().isEmpty());
        Assert.assertTrue(emptyFuture2.get().isEmpty());

        //Set clock to move onto the next window
        Mockito.when(clock.millis()).thenReturn(330L);

        Future<Set<ServiceMapRelationship>> frontHalfFuture = threadpool.submit(() -> {
            return serviceMapStateful1.execute(Collections.emptyList())
                    .stream()
                    .map(record -> {
                        try {
                            return OBJECT_MAPPER.readValue(record.getData(), ServiceMapRelationship.class);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    }).collect(Collectors.toSet());
        });

        Future<Set<ServiceMapRelationship>> backHalfFuture = threadpool.submit(() -> {
            return serviceMapStateful2.execute(Collections.emptyList())
                    .stream()
                    .map(record -> {
                        try {
                            return OBJECT_MAPPER.readValue(record.getData(), ServiceMapRelationship.class);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    }).collect(Collectors.toSet());
        });
        //Should iterate over the front half of the elements
        Set<ServiceMapRelationship> frontHalfRelationships = frontHalfFuture.get();
        //Should itearte over the back half of the elements
        Set<ServiceMapRelationship> backHalfRelationships = backHalfFuture.get();

        Assert.assertEquals(2, backHalfRelationships.size());
        Assert.assertTrue(backHalfRelationships.contains(relationship1));
        Assert.assertTrue(backHalfRelationships.contains(targetRelationship1));

        Assert.assertEquals(4, frontHalfRelationships.size());
        Assert.assertTrue(frontHalfRelationships.contains(relationship2));
        Assert.assertTrue(frontHalfRelationships.contains(targetRelationship2));
        Assert.assertTrue(frontHalfRelationships.contains(relationship3));
        Assert.assertTrue(frontHalfRelationships.contains(targetRelationship3));

        //As extra verification, evaluate the actual service map edges that result from the above responses
        //from the processor.
        Set<ServiceMapSourceDest> serviceMapSourceDests = evaluateEdges(new HashSet<ServiceMapRelationship>(){{
            addAll(frontHalfRelationships);
            addAll(backHalfRelationships);
        }});
        Assert.assertEquals(3, serviceMapSourceDests.size());
        Assert.assertTrue(serviceMapSourceDests.contains(new ServiceMapSourceDest(FRONTEND_SERVICE, BACKEND_SERVICE)));
        Assert.assertTrue(serviceMapSourceDests.contains(new ServiceMapSourceDest(BACKEND_SERVICE, DATABASE_SERVICE)));
        Assert.assertTrue(serviceMapSourceDests.contains(new ServiceMapSourceDest(BACKEND_SERVICE, CHECKOUT_SERVICE)));
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

