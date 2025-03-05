/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.parser;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler;
import com.fasterxml.jackson.databind.deser.ValueInstantiator;
import com.fasterxml.jackson.databind.util.ClassUtil;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;

@Named
public class DataPrepperDeserializationProblemHandler extends DeserializationProblemHandler {

    @Inject
    public DataPrepperDeserializationProblemHandler() {
        super();
    }

    @Override
    public Object handleWeirdStringValue(DeserializationContext ctxt, Class<?> targetType, String valueToConvert, String failureMsg) throws IOException {
        throw new IOException(
                String.format("Cannot deserialize value of type %s from String \"%s\": %s",
                        ClassUtil.nameOf(targetType), valueToConvert, failureMsg));
    }

    @Override
    public Object handleUnexpectedToken(DeserializationContext ctxt, JavaType targetType, JsonToken t, JsonParser p, String failureMsg) throws IOException {
        throw JsonMappingException.from(ctxt.getParser(),
                String.format("Cannot deserialize value of type %s from %s.",
                        ClassUtil.getTypeDescription(targetType), JsonToken.valueDescFor(t)));
    }

    @Override
    public Object handleMissingInstantiator(DeserializationContext ctxt, Class<?> instClass, ValueInstantiator valueInsta, JsonParser p, String msg) throws IOException {
        throw JsonMappingException.from(ctxt.getParser(),
                String.format("Cannot deserialize '%s' into '%s'.", p.getText(), instClass.getName()));
    }
}
