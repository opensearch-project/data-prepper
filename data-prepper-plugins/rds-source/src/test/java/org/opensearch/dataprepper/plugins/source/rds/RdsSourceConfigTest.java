/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.rds.configuration.ExportConfig;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

class RdsSourceConfigTest {

    @Test
    void test_default_RdsSourceConfig_returns_default_values() {
        final RdsSourceConfig objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.isAcknowledgmentsEnabled(), equalTo(true));
        assertThat(objectUnderTest.isDisableS3ReadForLeader(), equalTo(false));
    }

    @Test
    void test_when_export_is_not_configured_then_isExportEnabled_returns_false() {
        final RdsSourceConfig objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.isExportEnabled(), equalTo(false));
    }

    @Test
    void test_when_export_is_configured_then_isExportEnabled_returns_true() throws NoSuchFieldException, IllegalAccessException {
        final RdsSourceConfig objectUnderTest = createObjectUnderTest();

        ExportConfig exportConfig = new ExportConfig();
        setField(ExportConfig.class, exportConfig, "kmsKeyId", UUID.randomUUID().toString());
        setField(RdsSourceConfig.class, objectUnderTest, "exportConfig", exportConfig);

        assertThat(objectUnderTest.isExportEnabled(), equalTo(true));
    }

    private RdsSourceConfig createObjectUnderTest() {
        return new RdsSourceConfig();
    }
}