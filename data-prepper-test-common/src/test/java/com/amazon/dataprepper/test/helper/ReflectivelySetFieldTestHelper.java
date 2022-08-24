/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.test.helper;

public class ReflectivelySetFieldTestHelper {
    private String internalField = "first value";

    public String getInternalField() {
        return internalField;
    }
}
