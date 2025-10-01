/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.test.plugins.junit;

import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation to enable the Data Prepper plugin test framework
 * in JUnit.
 * <p>
 * Use {@link org.opensearch.dataprepper.test.plugins.DataPrepperPluginTest} instead.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@ExtendWith({
        DataPrepperPluginTestContextParameterResolver.class,
        PluginProviderParameterResolver.class
})
public @interface DataPrepperPluginTestFramework {
}
