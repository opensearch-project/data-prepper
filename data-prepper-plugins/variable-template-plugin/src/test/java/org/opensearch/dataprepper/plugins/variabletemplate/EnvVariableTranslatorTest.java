/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.variabletemplate;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.opensearch.dataprepper.plugins.variabletemplate.EnvVariableTranslator.PREFIX;

@ExtendWith(MockitoExtension.class)
class EnvVariableTranslatorTest {

    @Spy
    private EnvVariableTranslator objectUnderTest;

    @Test
    void testGetPrefix_returnsEnv() {
        assertThat(objectUnderTest.getPrefix(), equalTo(PREFIX));
    }

    @Test
    void testTranslate_returnsEnvValue() {
        doReturn("test-value").when(objectUnderTest).getenv("MY_VAR");
        assertThat(objectUnderTest.translate("MY_VAR"), equalTo("test-value"));
    }

    @Test
    void testTranslate_realEnvVar_returnsValue() {
        assertThat(objectUnderTest.translate("PATH"), equalTo(System.getenv("PATH")));
    }

    @Test
    void testTranslate_unsetVariable_throwsIllegalArgumentException() {
        final String varName = "__UNSET_" + UUID.randomUUID().toString().replace("-", "") + "__";
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> objectUnderTest.translate(varName));
        assertThat(ex.getMessage().contains(varName), equalTo(true));
    }

    @Test
    void testTranslateToPluginConfigVariable_returnsImmutableVariable() {
        doReturn("test-value").when(objectUnderTest).getenv("MY_VAR");
        final PluginConfigVariable variable = objectUnderTest.translateToPluginConfigVariable("MY_VAR");
        assertThat(variable, instanceOf(ImmutablePluginConfigVariable.class));
        assertThat(variable.getValue(), equalTo("test-value"));
        assertThat(variable.isUpdatable(), equalTo(false));
    }

    @Test
    void testTranslateToPluginConfigVariable_setValue_throwsException() {
        doReturn("test-value").when(objectUnderTest).getenv("MY_VAR");
        final PluginConfigVariable variable = objectUnderTest.translateToPluginConfigVariable("MY_VAR");
        assertThrows(Exception.class, () -> variable.setValue("new"));
    }

    @Test
    void testTranslateToPluginConfigVariable_refresh_isNoOp() {
        doReturn("test-value").when(objectUnderTest).getenv("MY_VAR");
        final PluginConfigVariable variable = objectUnderTest.translateToPluginConfigVariable("MY_VAR");
        variable.refresh();
        assertThat(variable.getValue(), equalTo("test-value"));
    }
}
