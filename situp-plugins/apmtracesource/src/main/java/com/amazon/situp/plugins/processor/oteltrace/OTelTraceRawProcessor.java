package com.amazon.situp.plugins.processor.oteltrace;

import com.amazon.situp.model.PluginType;
import com.amazon.situp.model.annotations.SitupPlugin;
import com.amazon.situp.model.processor.Processor;
import com.amazon.situp.model.record.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;


@SitupPlugin(name = "otel_trace_raw_processor", type = PluginType.PROCESSOR)
public class OTelTraceRawProcessor implements Processor<Record<ResourceSpans>, Record<String>> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String INSTRUMENTATION_LIBRARY_SPANS = "instrumentationLibrarySpans";
    private static final String INSTRUMENTATION_LIBRARY = "instrumentationLibrary";
    private static final String SPANS = "spans";
    private static final String RESOURCE = "resource";
    private static final String ATTRIBUTES = "attributes";
    private static final String START_TIME_UNIX_NANOS = "startTimeUnixNano";
    private static final String END_TIME_UNIX_NANOS = "endTimeUnixNano";
    private static final String START_TIME = "startTime";
    private static final String END_TIME = "endTime";
    private static final BigDecimal MILLIS_TO_NANOS = new BigDecimal(1_000_000);
    private static final BigDecimal SEC_TO_MILLIS = new BigDecimal(1_000);

    private static final Logger log = LoggerFactory.getLogger(OTelTraceRawProcessor.class);

    public static String getJsonFromProtobufObj(Record<ResourceSpans> resourceSpans) throws InvalidProtocolBufferException {
        return JsonFormat.printer().print(resourceSpans.getData());
    }

    public static ArrayList<String> decodeResourceSpan(final String jsonResourceSpans) throws JsonProcessingException {
        final JsonNode jsonNode = OBJECT_MAPPER.readTree(jsonResourceSpans);
        final ArrayList<String> result = new ArrayList<>(Collections.emptyList());
        //if number of spans is zero, return empty result.
        if (!jsonNode.path(INSTRUMENTATION_LIBRARY_SPANS).isArray())
            return result;
        final ArrayNode instrumentationLibrarySpans = (ArrayNode) jsonNode.path(INSTRUMENTATION_LIBRARY_SPANS);
        //Get Resource attributes, if not present we will store the spans without resources objects.
        final ArrayList<ObjectNode> resourceNodes = jsonNode.path(RESOURCE).path(ATTRIBUTES).isArray() ?
                processKeyValueList((ArrayNode) jsonNode.path(RESOURCE).path(ATTRIBUTES), String.format("%s.%s", RESOURCE, ATTRIBUTES), ".")
                : new ArrayList<>(Collections.emptyList());
        for (int i = 0; i < instrumentationLibrarySpans.size(); i++) {
            final ArrayNode spans = (ArrayNode) instrumentationLibrarySpans.get(i).path(SPANS);
            //if number of spans is zero, return empty result. Note this is a temporary implementation because in the final version we
            // will process the protobuf.
            if (!instrumentationLibrarySpans.get(i).path(SPANS).isArray())
                return new ArrayList<>(Collections.emptyList());
            for (int j = 0; j < spans.size(); j++) {
                final ObjectNode spanNode = (ObjectNode) spans.get(j);
                processDate(spanNode);
                //Get Span Attributes. Skipping Events/Links for now
                if (spanNode.path(ATTRIBUTES).isArray())
                    processKeyValueList((ArrayNode) spanNode.remove(ATTRIBUTES), ATTRIBUTES, ".").forEach(spanNode::setAll);
                resourceNodes.forEach(spanNode::setAll);
                if (!instrumentationLibrarySpans.get(i).path(INSTRUMENTATION_LIBRARY).isMissingNode())
                    spanNode.setAll((ObjectNode) instrumentationLibrarySpans.get(i).path(INSTRUMENTATION_LIBRARY));
                result.add(OBJECT_MAPPER.writeValueAsString(spanNode));
            }
        }
        return result;
    }

    private static ArrayList<ObjectNode> processKeyValueList(final ArrayNode resourceAttributes, final String prefix, final String fieldNameSeprator) {
        final ArrayList<ObjectNode> objectNodes = new ArrayList<>(Collections.emptyList());
        for (int i = 0; i < resourceAttributes.size(); i++) {
            final ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
            final String newKey = String.format("%s%s%s", prefix, fieldNameSeprator, resourceAttributes.get(i).get("key").asText());
            switch (resourceAttributes.get(i).get("value").fieldNames().next()) {
                case "stringValue":
                    objectNodes.add(objectNode.put(newKey, resourceAttributes.get(i).get("value").get("stringValue").asText()));
                    break;
                case "intValue":
                    objectNodes.add(objectNode.put(newKey, resourceAttributes.get(i).get("value").get("intValue").asLong()));
                    break;
                case "boolValue":
                    objectNodes.add(objectNode.put(newKey, resourceAttributes.get(i).get("value").get("boolValue").asBoolean()));
                    break;
                case "doubleValue":
                    objectNodes.add(objectNode.put(newKey, resourceAttributes.get(i).get("value").get("doubleValue").asDouble()));
                    break;
                case "arrayValue":
                    objectNodes.add(objectNode.set(newKey, resourceAttributes.get(i).get("value").get("arrayValue")));
                    break;
                case "keyValueList":
                    //TBD: This flatten is something we will not do, but keeping it here.
                    objectNodes.addAll(processKeyValueList((ArrayNode) resourceAttributes.get(i).get("value"), newKey, "_"));
                    break;
            }
        }
        return objectNodes;
    }

    private static void processDate(final ObjectNode spanNode) {
        try {
            final String startTimeUnixNano = spanNode.remove(START_TIME_UNIX_NANOS).asText();
            final String endTimeUnixNano = spanNode.remove(END_TIME_UNIX_NANOS).asText();
            spanNode.put(START_TIME, convertStringNanosToISO8601(startTimeUnixNano));
            spanNode.put(END_TIME, convertStringNanosToISO8601(endTimeUnixNano));
        } catch (Exception ex) {
            //do nothing, just move forward.
        }
    }

    private static String convertStringNanosToISO8601(final String stringNanos) {
        final BigDecimal nanos = new BigDecimal(stringNanos);
        final long epochSeconds = nanos.divide(MILLIS_TO_NANOS.multiply(SEC_TO_MILLIS), RoundingMode.DOWN).longValue();
        final int nanoAdj = nanos.remainder(MILLIS_TO_NANOS.multiply(SEC_TO_MILLIS)).intValue();
        return Instant.ofEpochSecond(epochSeconds, nanoAdj).toString();
    }

    /**
     * execute the processor logic which could potentially modify the incoming record. The level to which the record has
     * been modified depends on the implementation
     *
     * @param records Input records that will be modified/processed
     * @return Record  modified output records
     */
    @Override
    public Collection<Record<String>> execute(Collection<Record<ResourceSpans>> records) {
        final List<Record<String>> finalRecords = new LinkedList<>();
        for (Record<ResourceSpans> rs : records) {
            String jsonString;
            ArrayList<String> esDocs = null;
            try {
                jsonString = getJsonFromProtobufObj(rs);
                esDocs = decodeResourceSpan(jsonString);
            } catch (InvalidProtocolBufferException | JsonProcessingException e) {
                log.warn("Unable to process invalid records", e);
            }
            finalRecords.add(new Record(esDocs));
        }
        return finalRecords;
    }
}
