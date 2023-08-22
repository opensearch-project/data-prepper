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

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class KeyConfigTest {

    public static final String SOURCE = "/peer/ip";
    public static final String TARGET = "location";
    private KeyConfig keyConfig;

    @BeforeEach
    void setUp() {
        keyConfig = new KeyConfig();
    }

    @Test
    void getSourceTestPositive() throws NoSuchFieldException, IllegalAccessException {
        ReflectivelySetField.setField(KeyConfig.class, keyConfig, "source", SOURCE);
        assertThat(keyConfig.getSource(), equalTo(SOURCE));
    }

    @Test
    void getSourceTestNegative() {
        assertThat(new KeyConfig().getSource(), equalTo(null));
    }

    @Test
    void getTargetTestPositive() throws NoSuchFieldException, IllegalAccessException {
        ReflectivelySetField.setField(KeyConfig.class, keyConfig, "target", TARGET);
        assertThat(keyConfig.getTarget(), equalTo(TARGET));
    }

    @Test
    void getTargetTestNegative() {
        assertThat(new KeyConfig().getTarget(), equalTo(null));
    }

    @Test
    void getAttributesTestPositive() throws NoSuchFieldException, IllegalAccessException {
        final List<String> attributes = setAttributes();
        ReflectivelySetField.setField(KeyConfig.class, keyConfig, "attributes", attributes);
        assertThat(keyConfig.getAttributes().get(0), equalTo(attributes.get(0)));
    }

    @Test
    void getAttributesTestNegative() {
        assertThat(new KeyConfig().getAttributes(), equalTo(null));
    }

    private List<String> setAttributes() {
        final List<String> attributes = new ArrayList<>();
        attributes.add("city_name");
        attributes.add("country_name");
        return attributes;
    }
}