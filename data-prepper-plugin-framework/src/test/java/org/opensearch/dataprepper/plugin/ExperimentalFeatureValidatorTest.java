/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugin;

import jakarta.validation.ConstraintValidatorContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExperimentalFeatureValidatorTest {

    @Mock
    private ExperimentalConfigurationContainer experimentalConfigurationContainer;

    @Mock
    private ExperimentalConfiguration experimentalConfiguration;

    @Mock
    private ConstraintValidatorContext context;

    @BeforeEach
    void setUp() {
        when(experimentalConfigurationContainer.getExperimental()).thenReturn(experimentalConfiguration);
    }

    private ExperimentalFeatureValidator createObjectUnderTest() {
        return new ExperimentalFeatureValidator(experimentalConfigurationContainer);
    }

    @Test
    void isValid_returns_true_when_value_is_null() {
        assertThat(createObjectUnderTest().isValid(null, context), equalTo(true));
    }

    @Test
    void isValid_returns_true_when_value_is_non_null_and_enableAll_is_true() {
        when(experimentalConfiguration.isEnableAll()).thenReturn(true);

        assertThat(createObjectUnderTest().isValid(new Object(), context), equalTo(true));
    }

    @Test
    void isValid_returns_false_when_value_is_non_null_and_enableAll_is_false() {
        when(experimentalConfiguration.isEnableAll()).thenReturn(false);

        assertThat(createObjectUnderTest().isValid(new Object(), context), equalTo(false));
    }
}
