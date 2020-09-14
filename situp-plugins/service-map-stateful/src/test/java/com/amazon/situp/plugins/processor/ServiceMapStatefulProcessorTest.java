package com.amazon.situp.plugins.processor;

import com.google.common.base.Charsets;
import com.google.protobuf.ByteString;
import com.amazon.situp.model.record.Record;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
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
    public ResourceSpans getResourceSpans(final String serviceName, final String spanName, final String
            spanId, final String parentId, final Span.SpanKind spanKind) throws UnsupportedEncodingException {
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
                                                .setSpanId(ByteString.copyFrom(spanId, Charsets.UTF_8.name()))
                                                .setParentSpanId(ByteString.copyFrom(parentId, Charsets.UTF_8.name()))
                                                .build()
                                )
                                .build()
                )
                .build();
    }

    /**
     * Test with the following fake trace setup
     *
     * |----  Service = frontend ------------------------------------------------|
     *   |--  Service = backend --------------------------------------------|
     *     |-- Service = database --|    |-- Id=4, service = checkout---|
     *
     *                                                                                    |--Id=5, service = nothing--|
     */
    @Test
    public void testSimpleSpansProcessing() throws IOException {
        final ServiceMapStatefulProcessor serviceMapStateful = new ServiceMapStatefulProcessor(1, temporaryFolder.newFolder());
        try {
            //Fake span data

            //Frontend service client span
            ResourceSpans spans1 = getResourceSpans(FRONTEND_SERVICE, "span1", "1", "1", Span.SpanKind.SPAN_KIND_CLIENT);
            //Backend service server and client spans
            ResourceSpans spans2 = getResourceSpans(BACKEND_SERVICE, "span2", "2", "1", Span.SpanKind.SPAN_KIND_SERVER);
            ResourceSpans spans3 = getResourceSpans(BACKEND_SERVICE, "span3", "3", "2", Span.SpanKind.SPAN_KIND_CLIENT);
            //Database service server span
            ResourceSpans spans4 = getResourceSpans(DATABASE_SERVICE, "span4" ,"4","3", Span.SpanKind.SPAN_KIND_SERVER );
            //Checkoue service server span
            ResourceSpans spans5 = getResourceSpans(CHECKOUT_SERVICE, "span5","5", "3", Span.SpanKind.SPAN_KIND_SERVER);
            //Other service span
            ResourceSpans spans6 = getResourceSpans(OTHER_SERVICE, "span6","6","7", Span.SpanKind.SPAN_KIND_UNSPECIFIED);

            //Expected relationships
            final ServiceMapRelationship relationship1 = new ServiceMapRelationship(FRONTEND_SERVICE, Span.SpanKind.SPAN_KIND_CLIENT.name(), BACKEND_SERVICE, null);
            final ServiceMapRelationship relationship2 = new ServiceMapRelationship(BACKEND_SERVICE, Span.SpanKind.SPAN_KIND_CLIENT.name(), DATABASE_SERVICE, null);
            final ServiceMapRelationship relationship3 = new ServiceMapRelationship(BACKEND_SERVICE, Span.SpanKind.SPAN_KIND_CLIENT.name(), CHECKOUT_SERVICE, null);
            final ServiceMapRelationship targetRelationship1 = new ServiceMapRelationship(BACKEND_SERVICE, Span.SpanKind.SPAN_KIND_SERVER.name(), null, BACKEND_SERVICE);
            final ServiceMapRelationship targetRelationship2 = new ServiceMapRelationship(DATABASE_SERVICE, Span.SpanKind.SPAN_KIND_SERVER.name(), null, DATABASE_SERVICE);
            final ServiceMapRelationship targetRelationship3 = new ServiceMapRelationship(CHECKOUT_SERVICE, Span.SpanKind.SPAN_KIND_SERVER.name(), null, CHECKOUT_SERVICE);

            //Sleep for 1.2 seconds, to close out the original window
            Thread.sleep(1200);

            //Process Span 1, should result in no relationships found
            final Collection<Record<String>> emptyRelationships = serviceMapStateful.execute(Arrays.asList(
                    new Record<>(spans1)
            ));

            Assert.assertTrue(emptyRelationships.isEmpty());

            //Sleep for 1.2 seconds, to move on to the next window
            Thread.sleep(1200);

            //Process spans 2, 3, and 4. This should lead to detection of 4 relationships
            final Set<ServiceMapRelationship> relationships1 = serviceMapStateful.execute(Arrays.asList(
                    new Record<>(spans2),
                    new Record<>(spans3),
                    new Record<>(spans4)
            ))
                    .stream()
                    .map(record -> {
                        try {
                            return OBJECT_MAPPER.readValue(record.getData(), ServiceMapRelationship.class);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    }).collect(Collectors.toSet());

            Assert.assertEquals(4, relationships1.size());
            Assert.assertTrue(relationships1.contains(relationship1));
            Assert.assertTrue(relationships1.contains(relationship2));
            Assert.assertTrue(relationships1.contains(targetRelationship1));
            Assert.assertTrue(relationships1.contains(targetRelationship2));

            //Sleep for 1.2 seconds, so close out the window. The original window with span 1 is now expired and deleted
            Thread.sleep(1200);

            //Process spans 5 and 6. Should lead to detection of 2 relationships
            final Set<ServiceMapRelationship> relationships2 = serviceMapStateful.execute(Arrays.asList(
                    new Record<>(spans5),
                    new Record<>(spans6)
            )).stream()
                    .map(record -> {
                        try {
                            return OBJECT_MAPPER.readValue(record.getData(), ServiceMapRelationship.class);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    }).collect(Collectors.toSet());

            Assert.assertEquals(2, relationships2.size());
            Assert.assertTrue(relationships2.contains(relationship3));
            Assert.assertTrue(relationships2.contains(targetRelationship3));

            //As extra verification, evaluate the actual service map edges that result from the above responses
            //from the processor.
            Set<ServiceMapSourceDest> serviceMapSourceDests = evaluateEdges(new HashSet<ServiceMapRelationship>(){{
                addAll(relationships1);
                addAll(relationships2);
            }});
            Assert.assertEquals(3, serviceMapSourceDests.size());
            Assert.assertTrue(serviceMapSourceDests.contains(new ServiceMapSourceDest(FRONTEND_SERVICE, BACKEND_SERVICE)));
            Assert.assertTrue(serviceMapSourceDests.contains(new ServiceMapSourceDest(BACKEND_SERVICE, DATABASE_SERVICE)));
            Assert.assertTrue(serviceMapSourceDests.contains(new ServiceMapSourceDest(BACKEND_SERVICE, CHECKOUT_SERVICE)));


        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
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

