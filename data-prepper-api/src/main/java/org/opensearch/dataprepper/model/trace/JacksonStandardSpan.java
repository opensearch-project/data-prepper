/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.trace;

import org.opensearch.dataprepper.model.validation.ParameterValidator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class JacksonStandardSpan extends JacksonSpan {
    protected JacksonStandardSpan(final Builder builder) {
        super(builder);
    }
    
    public static JacksonStandardSpan.Builder builder() {
        return new JacksonStandardSpan.Builder();
    }

    @Override
    protected void validateParameters() {
        new ParameterValidator().validate(Collections.emptyList(), REQUIRED_NON_EMPTY_KEYS, Collections.emptyList(), (HashMap<String, Object>)toMap());
    }

    @Override
    protected void checkAndSetDefaultValues() {
        putIfAbsent(ATTRIBUTES_KEY, Map.class, new HashMap<>());
    }

    @Override
    public String toJsonString() {
        return getJsonNode().toString();
    }

    public static class Builder extends JacksonSpan.Builder {
        @Override
        public Builder withTraceGroup(final String traceGroup) {
            return this;
        }

        @Override
        public Builder withTraceGroupFields(final TraceGroupFields traceGroupFields) {
            return this;
        }

        @Override
        public JacksonSpan build() {
            populateEvent();
            return new JacksonStandardSpan(this);
        }
    }
}
