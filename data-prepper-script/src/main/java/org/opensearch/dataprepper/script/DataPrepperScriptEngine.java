/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.script;

import org.opensearch.dataprepper.script.parser.ScriptParser;

public class DataPrepperScriptEngine implements ScriptEngine {
    private final ScriptParser parser;

    public DataPrepperScriptEngine(final ScriptParser parser) {
        this.parser = parser;
    }
}
