/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.plugin;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineTransformFunctionProviderTest {

    @Test
    void implementing_class_is_assignable_from_interface() {
        PipelineTransformFunctionProvider provider = new TestFunctionProvider();
        assertNotNull(provider);
        assertTrue(provider instanceof PipelineTransformFunctionProvider);
    }

    @Test
    void interface_is_assignable_from_implementing_class() {
        assertThat(PipelineTransformFunctionProvider.class.isAssignableFrom(TestFunctionProvider.class),
                equalTo(true));
    }

    @Test
    void interface_is_not_assignable_from_non_implementing_class() {
        assertThat(PipelineTransformFunctionProvider.class.isAssignableFrom(NonProvider.class),
                equalTo(false));
    }

    @Test
    void interface_has_no_declared_methods() {
        assertThat(PipelineTransformFunctionProvider.class.getDeclaredMethods().length, equalTo(0));
    }

    @Test
    void interface_is_public() {
        assertTrue(java.lang.reflect.Modifier.isPublic(PipelineTransformFunctionProvider.class.getModifiers()));
    }

    @Test
    void interface_is_an_interface() {
        assertTrue(PipelineTransformFunctionProvider.class.isInterface());
    }

    static class TestFunctionProvider implements PipelineTransformFunctionProvider {
        public static String sampleFunction(String input) {
            return input;
        }
    }

    static class NonProvider {
        public static String someMethod(String input) {
            return input;
        }
    }
}
