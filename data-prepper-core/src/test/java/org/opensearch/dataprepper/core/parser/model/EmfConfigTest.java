/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.parser.model;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class EmfConfigTest {

    @Test
    public void testDefaultConstructor() {
        final EmfConfig emfConfiguration = new EmfConfig();
        assertThat(emfConfiguration.getAdditionalProperties(), equalTo(Collections.emptyMap()));
    }
}
