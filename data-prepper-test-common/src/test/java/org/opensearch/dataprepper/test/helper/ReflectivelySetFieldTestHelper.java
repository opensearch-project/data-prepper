/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.helper;

public class ReflectivelySetFieldTestHelper {
    private String internalField = "first value";

    public String getInternalField() {
        return internalField;
    }
}
