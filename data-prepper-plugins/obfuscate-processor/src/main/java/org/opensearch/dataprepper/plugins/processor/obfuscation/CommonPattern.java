/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.obfuscation;

enum CommonPattern {
    EMAIL_ADDRESS("[A-Za-z0-9+_.-]+@([\\w-]+\\.)+[\\w-]{2,4}"),
    IP_ADDRESS_V4("((25[0-5]|(2[0-4]|1\\d|[1-9]|)\\d)\\.?\\b){4}"),
    BASE_NUMBER("[0-9]*\\.?[0-9]+"),
    CREDIT_CARD_NUMBER("(\\d[ -]*?){13,16}"),
    US_PHONE_NUMBER("\\+?\\d?[\\s-]?(\\(\\d{3}\\)|\\d{3})[\\s-]?\\d{3}[\\s-]?\\d{4}"),
    US_SSN_NUMBER("[0-9]{3}-[0-9]{2}-[0-9]{4}");

    final String expr;

    CommonPattern(String expr) {
        this.expr = expr;
    }

    public String getExpr() {
        return expr;
    }
}
