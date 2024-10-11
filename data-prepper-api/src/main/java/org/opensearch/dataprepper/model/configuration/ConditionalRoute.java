/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.configuration;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Model representing a route within a pipeline.
 *
 * @since 2.0
 */
@JsonPropertyOrder
@JsonClassDescription("The key-value pair defines routing condition, where the key is the name of a route and the " +
        "value is an expression representing the routing condition.")
@JsonSerialize(using = ConditionalRoute.ConditionalRouteSerializer.class)
@JsonDeserialize(using = ConditionalRoute.ConditionalRouteDeserializer.class)
public class ConditionalRoute {
    private final String name;
    private final String condition;

    @JsonCreator
    public ConditionalRoute(final String name, final String condition) {
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

    static class ConditionalRouteSerializer extends StdSerializer<ConditionalRoute> {

        protected ConditionalRouteSerializer() {
            super(ConditionalRoute.class);
        }

        @Override
        public void serialize(final ConditionalRoute value, final JsonGenerator gen, final SerializerProvider provider) throws IOException {
            gen.writeStartObject();
            gen.writeObjectField(value.name, value.condition);
            gen.writeEndObject();
        }
    }

    static class ConditionalRouteDeserializer extends StdDeserializer<ConditionalRoute> {

        protected ConditionalRouteDeserializer() {
            super(ConditionalRoute.class);
        }

        @Override
        public ConditionalRoute deserialize(final JsonParser parser, final DeserializationContext context) throws IOException, JacksonException {
            final JsonNode node = context.readTree(parser);

            final Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            final Map.Entry<String, JsonNode> onlyField = fields.next();

            final String routeName = onlyField.getKey();
            final JsonNode value = onlyField.getValue();
            if(!value.isTextual())
                throw new InvalidFormatException(parser, "Route has a condition which is not a string.", value, String.class);
            final String condition = value.asText();

            if(fields.hasNext())
                throw new InvalidFormatException(parser, "Route has too many fields.", null, String.class);

            return new ConditionalRoute(routeName, condition);
        }
    }
}
