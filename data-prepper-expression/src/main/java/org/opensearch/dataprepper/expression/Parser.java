package org.opensearch.dataprepper.expression;

interface Parser<T> {

    /**
     * @since 1.3
     * Parse a statement String to an object that can be evaluated by an {@link Evaluator}
     * @param statement String to be parsed
     * @return Object representing a parsed statement
     */
    T parse(final String statement);
}
