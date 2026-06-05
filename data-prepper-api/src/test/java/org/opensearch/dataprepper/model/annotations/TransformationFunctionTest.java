/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.annotations;

import org.junit.jupiter.api.Test;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransformationFunctionTest {

    @Test
    void annotation_is_retained_at_runtime() {
        Retention retention = TransformationFunction.class.getAnnotation(Retention.class);
        assertNotNull(retention);
        assertThat(retention.value(), equalTo(RetentionPolicy.RUNTIME));
    }

    @Test
    void annotation_targets_methods() {
        Target target = TransformationFunction.class.getAnnotation(Target.class);
        assertNotNull(target);
        assertThat(target.value().length, equalTo(1));
        assertThat(target.value()[0], equalTo(ElementType.METHOD));
    }

    @Test
    void annotation_is_documented() {
        Documented documented = TransformationFunction.class.getAnnotation(Documented.class);
        assertNotNull(documented);
    }

    @Test
    void annotation_is_present_on_annotated_method() throws NoSuchMethodException {
        assertTrue(AnnotatedClass.class.getMethod("annotatedMethod", String.class)
                .isAnnotationPresent(TransformationFunction.class));
    }

    @Test
    void annotation_is_not_present_on_unannotated_method() throws NoSuchMethodException {
        assertTrue(!AnnotatedClass.class.getMethod("unannotatedMethod", String.class)
                .isAnnotationPresent(TransformationFunction.class));
    }

    static class AnnotatedClass {
        @TransformationFunction
        public static String annotatedMethod(String input) {
            return input;
        }

        public static String unannotatedMethod(String input) {
            return input;
        }
    }
}
