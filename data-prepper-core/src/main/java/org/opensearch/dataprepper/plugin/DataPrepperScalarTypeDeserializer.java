/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

public class DataPrepperScalarTypeDeserializer<T> extends JsonDeserializer<T> {
    private final VariableExpander variableExpander;
    private final Class<T> scalarType;

    public DataPrepperScalarTypeDeserializer(final VariableExpander variableExpander, final Class<T> scalarType) {
        this.variableExpander = variableExpander;
        this.scalarType = scalarType;
    }

    @Override
    public T deserialize(final JsonParser jsonParser, final DeserializationContext ctxt) throws IOException {
        return variableExpander.translate(jsonParser, this.scalarType);
    }
}
