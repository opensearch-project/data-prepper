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
class KeysConfigTest {

    private KeysConfig keysConfig;
    private final KeyConfig keyConfig = new KeyConfig();

    @BeforeEach
    void setUp() {
        keysConfig = new KeysConfig();
    }

    @Test
    void getKeyConfigTestPositive() throws NoSuchFieldException, IllegalAccessException {
        ReflectivelySetField.setField(KeysConfig.class, keysConfig, "keyConfig", keyConfig);
        assertThat(keysConfig.getKeyConfig(), equalTo(keyConfig));
    }

    @Test
    void getKeyConfigTestNegative() {
        assertThat(new KeysConfig().getKeyConfig(), equalTo(null));
    }
}
