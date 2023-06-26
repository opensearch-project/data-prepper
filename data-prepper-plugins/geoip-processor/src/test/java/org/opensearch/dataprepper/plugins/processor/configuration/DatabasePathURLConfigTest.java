/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class DatabasePathURLConfigTest {

    private static final String URL = "s3://mybucket10012023/GeoLite2";
    private DatabasePathURLConfig databasePathURLConfig;

    @BeforeEach
    void setUp() {
        databasePathURLConfig = new DatabasePathURLConfig();
    }

    @Test
    void getUrlTestPositive() throws NoSuchFieldException, IllegalAccessException {
        ReflectivelySetField.setField(DatabasePathURLConfig.class, databasePathURLConfig, "url", URL);
        assertThat(databasePathURLConfig.getUrl(), equalTo(URL));
    }

    @Test
    void getUrlTestNegative() {
        assertThat(new DatabasePathURLConfig().getUrl(), equalTo(null));
    }
}
