/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Model representing a route within a pipeline.
 *
 * @since 2.0
 */
@JsonSerialize(using = PipelineConditionalRoute.PipelineConditionalRouteSerializer.class)
@JsonDeserialize(using = PipelineConditionalRoute.PipelineConditionalRouteDeserializer.class)
public class PipelineConditionalRoute {
    private final String name;
    private final String condition;

    @JsonCreator
    public PipelineConditionalRoute(final String name, final String condition) {
        this.name = name;
        this.condition = condition;
    }

    /**
     * Gets the name of the route.
     *
     * @return the route name
     * @since 2.0
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the condition which applies for this route.
     *
     * @return the condition
     * @since 2.0
     */
    public String getCondition() {
        return condition;
    }

    static class PipelineConditionalRouteSerializer extends StdSerializer<PipelineConditionalRoute> {

        protected PipelineConditionalRouteSerializer() {
            super(PipelineConditionalRoute.class);
        }

        @Override
        public void serialize(final PipelineConditionalRoute value, final JsonGenerator gen, final SerializerProvider provider) throws IOException {
            gen.writeStartObject();
            gen.writeObjectField(value.name, value.condition);
            gen.writeEndObject();
        }
    }

    static class PipelineConditionalRouteDeserializer extends StdDeserializer<PipelineConditionalRoute> {

        protected PipelineConditionalRouteDeserializer() {
            super(PipelineConditionalRoute.class);
        }

        @Override
        public PipelineConditionalRoute deserialize(final JsonParser parser, final DeserializationContext context) throws IOException, JacksonException {
            final JsonNode node = parser.getCodec().readTree(parser);

            final Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            final Map.Entry<String, JsonNode> onlyField = fields.next();

            final String routeName = onlyField.getKey();
            final JsonNode value = onlyField.getValue();
            final String condition = value.asText();

            return new PipelineConditionalRoute(routeName, condition);
        }
    }
}
