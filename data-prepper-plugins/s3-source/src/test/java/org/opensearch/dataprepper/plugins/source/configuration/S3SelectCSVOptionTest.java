/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class S3SelectCSVOptionTest {
    @Test
    void s3SelectOptionsTest() throws NoSuchFieldException, IllegalAccessException {
        S3SelectCSVOption csvOption = new S3SelectCSVOption();
        ReflectivelySetField.setField(S3SelectCSVOption.class,csvOption,"quiteEscape",",");
        ReflectivelySetField.setField(S3SelectCSVOption.class,csvOption,"comments","test");
        assertThat(csvOption.getFileHeaderInfo(),equalTo("USE"));
        assertThat(csvOption.getComments(),equalTo("test"));
        assertThat(csvOption.getQuiteEscape(),equalTo(","));
    }
}
