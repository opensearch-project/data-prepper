/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.configuration;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class DataPrepperVersionIT {
    @Test
    void getCurrentVersion_returns_expected_value_using_Java_SPI() {
        final String fullDataPrepperVersion = System.getProperty("project.version");

        final DataPrepperVersion currentVersion = DataPrepperVersion.getCurrentVersion();
        assertThat(currentVersion, notNullValue());
        assertThat(fullDataPrepperVersion, containsString(currentVersion.toString()));
        assertThat(currentVersion, equalTo(DataPrepperVersion.parse(fullDataPrepperVersion)));
    }
}
