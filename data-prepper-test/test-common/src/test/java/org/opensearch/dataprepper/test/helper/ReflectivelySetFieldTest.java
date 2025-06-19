/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.helper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

class ReflectivelySetFieldTest {
    private ReflectivelySetFieldTestHelper configuration;

    @BeforeEach
    void setup() {
        configuration = new ReflectivelySetFieldTestHelper();
    }

    @Test
    void test_when_setFieldOnExistingField_then_setsFieldAndNotAccessibleAfterwards() throws NoSuchFieldException, IllegalAccessException {
        assertThat(configuration.getInternalField(), equalTo("first value"));
        setField(ReflectivelySetFieldTestHelper.class, configuration, "internalField", "second value");
        assertThat(configuration.getInternalField(), equalTo("second value"));

        assertThrows(IllegalAccessException.class,  () -> ReflectivelySetFieldTestHelper.class.getDeclaredField("internalField")
                .set(configuration, "third value"));
    }

    @Test
    void test_when_setFieldThatDoesNotExist_then_throwsNoSuchFieldException() {
        assertThrows(NoSuchFieldException.class, () -> setField(ReflectivelySetFieldTestHelper.class, configuration,
                "fieldThatDNE", "value"));
    }
}
