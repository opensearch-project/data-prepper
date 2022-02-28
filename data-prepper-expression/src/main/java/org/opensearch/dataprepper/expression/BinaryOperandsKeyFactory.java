/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.expression;

class BinaryOperandsKeyFactory {
    static String typesKey(final Object leftValue, final Object rightValue) {
        return leftValue.getClass().getName() + "_" + rightValue.getClass().getName();
    }
}
