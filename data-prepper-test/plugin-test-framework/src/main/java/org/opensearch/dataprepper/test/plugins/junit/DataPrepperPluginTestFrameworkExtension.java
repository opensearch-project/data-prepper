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
import org.opensearch.dataprepper.plugin.ClasspathPluginProvider;
import org.opensearch.dataprepper.plugin.PluginProvider;
import org.opensearch.dataprepper.test.plugins.DataPrepperPluginTest;

import java.util.Set;

/**
 * A JUnit extension for using the Data Prepper plugin test framework.
 */
@Deprecated
public class DataPrepperPluginTestFrameworkExtension implements ParameterResolver {
    private static final Set<Class<?>> SUPPORTED_CLASSES = Set.of(
            DataPrepperPluginTestContext.class,
            PluginProvider.class
    );

    @Override
    public boolean supportsParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext) throws ParameterResolutionException {
        return SUPPORTED_CLASSES.contains(parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(
            final ParameterContext parameterContext,
            final ExtensionContext extensionContext) throws ParameterResolutionException {
        final Class<?> type = parameterContext.getParameter().getType();

        if (type.equals(DataPrepperPluginTestContext.class)) {
            final Class<?> testClass = extensionContext.getRequiredTestClass();
            final DataPrepperPluginTest annotation = testClass.getAnnotation(DataPrepperPluginTest.class);
            if (annotation == null) {
                throw new ParameterResolutionException("Missing @DataPrepperPluginTest annotation on class: " + testClass.getName());
            }
            return new DataPrepperPluginTestContext(annotation.pluginName(), annotation.pluginType());
        } else if (type.equals(PluginProvider.class)) {
            return new ClasspathPluginProvider();
        }

        throw new ParameterResolutionException("Unsupported parameter type: " + type);
    }
}
