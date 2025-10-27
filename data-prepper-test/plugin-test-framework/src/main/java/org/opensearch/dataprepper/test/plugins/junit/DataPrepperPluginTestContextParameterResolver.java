/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.test.plugins.junit;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.opensearch.dataprepper.test.plugins.DataPrepperPluginTest;

class DataPrepperPluginTestContextParameterResolver implements ParameterResolver {
    @Override
    public boolean supportsParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext) throws ParameterResolutionException {
        return DataPrepperPluginTestContext.class.equals(parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext) throws ParameterResolutionException {
        final Class<?> testClass = extensionContext.getRequiredTestClass();
        final DataPrepperPluginTest annotation = testClass.getAnnotation(DataPrepperPluginTest.class);
        if (annotation == null) {
            throw new ParameterResolutionException("Missing @DataPrepperPluginTest annotation on class: " + testClass.getName());
        }
        return new DataPrepperPluginTestContext(annotation.pluginName(), annotation.pluginType());
    }
}
