/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.script;

import com.amazon.dataprepper.model.event.Event;
import org.opensearch.dataprepper.script.parser.ScriptParser;

public class DataPrepperScriptEngine implements ScriptEngine {
    private final ScriptParser parser;

    public DataPrepperScriptEngine(final ScriptParser parser) {
        this.parser = parser;
    }

    /**
     * {@inheritDoc}
     *
     * <b>Method not implemented</b>
     */
    @Override
    public Object evaluate(final String statement, final Event context) {
        throw new RuntimeException("Method not implemented");
    }

    /**
     * {@inheritDoc}
     *
     * <b>Method not implemented</b>
     */
    @Override
    public <T> T evaluate(final String statement, final Event context, final Class<T> toValueType) throws ClassCastException {
        throw new RuntimeException("Method not implemented");
    }
}
