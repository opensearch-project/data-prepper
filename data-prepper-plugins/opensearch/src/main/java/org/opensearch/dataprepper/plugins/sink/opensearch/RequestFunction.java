/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

/**
 * Represents a function that accepts one argument and produces a result or throws Exception.
 *
 * @param <T> the type of the input to the function
 * @param <R> the type of the result of the function
 */
@FunctionalInterface
public interface RequestFunction<T, R> {

    R apply(T t) throws Exception;
}
