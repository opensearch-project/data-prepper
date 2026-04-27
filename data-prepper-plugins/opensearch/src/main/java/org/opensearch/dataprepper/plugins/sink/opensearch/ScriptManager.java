/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import com.fasterxml.jackson.databind.JsonNode;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch._types.InlineScript;
import org.opensearch.client.opensearch._types.Script;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.ScriptConfiguration;

import java.util.HashMap;
import java.util.Map;

public class ScriptManager {

    private static final String PAINLESS = "painless";
    private static final String DOC_PARAM = "doc";

    private final ScriptConfiguration scriptConfiguration;
    private final ExpressionEvaluator expressionEvaluator;

    public ScriptManager(final ScriptConfiguration scriptConfiguration, final ExpressionEvaluator expressionEvaluator) {
        this.scriptConfiguration = scriptConfiguration;
        this.expressionEvaluator = expressionEvaluator;
    }

    public boolean isScriptEnabled() {
        return scriptConfiguration != null;
    }

    public Map<String, Object> resolveParams(final Event event) {
        if (scriptConfiguration == null || scriptConfiguration.getParams() == null) {
            return null;
        }
        final Map<String, Object> resolved = new HashMap<>();
        for (final Map.Entry<String, Object> entry : scriptConfiguration.getParams().entrySet()) {
            final Object value = entry.getValue();
            if (value instanceof String && ((String) value).contains("${")) {
                resolved.put(entry.getKey(), event.formatString((String) value, expressionEvaluator));
            } else {
                resolved.put(entry.getKey(), value);
            }
        }
        return resolved;
    }

    public Script buildScript(final JsonNode jsonNode, final Map<String, Object> resolvedParams) {
        final Map<String, JsonData> scriptParams = new HashMap<>();
        scriptParams.put(DOC_PARAM, JsonData.of(jsonNode));
        if (resolvedParams != null) {
            resolvedParams.forEach((k, v) -> scriptParams.put(k, JsonData.of(v)));
        }
        final InlineScript inlineScript = new InlineScript.Builder()
                .source(scriptConfiguration.getSource())
                .lang(PAINLESS)
                .params(scriptParams)
                .build();
        return Script.of(s -> s.inline(inlineScript));
    }
}
