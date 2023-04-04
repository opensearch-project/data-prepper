/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class S3SelectJsonOptionTest {

    @Test
    void s3SelectJsonOptionWithLinesTest() throws NoSuchFieldException, IllegalAccessException {
        S3SelectJsonOption jsonOption = new S3SelectJsonOption();
        ReflectivelySetField.setField(S3SelectJsonOption.class,jsonOption,"type","Lines");
        assertThat(jsonOption.getType(),equalTo("Lines"));
    }

    @Test
    void s3SelectJsonOptionWithDefaultOptionTest() throws NoSuchFieldException, IllegalAccessException {
        S3SelectJsonOption jsonOption = new S3SelectJsonOption();
        assertThat(jsonOption.getType(),equalTo("DOCUMENT"));
    }
}
