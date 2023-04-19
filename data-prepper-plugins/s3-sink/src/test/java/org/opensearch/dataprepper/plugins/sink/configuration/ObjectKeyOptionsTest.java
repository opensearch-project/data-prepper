/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.configuration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Test;

class ObjectKeyOptionsTest {

    private static final String DEFAULT_FILE_PATTERN = "events-%{yyyy-MM-dd'T'hh-mm-ss}";

    @Test
    void default_file_pattern_test() {
        assertThat(new ObjectKeyOptions().getNamePattern(), equalTo(DEFAULT_FILE_PATTERN));
    }

    @Test
    void default_path_prefix_test() {
        assertThat(new ObjectKeyOptions().getPathPrefix(), equalTo(null));
    }
}