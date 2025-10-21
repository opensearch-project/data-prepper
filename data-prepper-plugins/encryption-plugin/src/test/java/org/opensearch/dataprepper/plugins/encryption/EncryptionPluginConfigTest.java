/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class EncryptionPluginConfigTest {
    @Test
    void testDefault() {
        final EncryptionPluginConfig encryptionPluginConfig = new EncryptionPluginConfig();
        assertThat(encryptionPluginConfig.getEncryptionConfigurationMap(), equalTo(Collections.emptyMap()));
    }
}