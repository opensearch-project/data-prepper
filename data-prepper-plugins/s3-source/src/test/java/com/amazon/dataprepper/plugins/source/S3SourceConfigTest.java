/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.plugins.source.configuration.CompressionOption;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class S3SourceConfigTest {

    @Test
    void default_compression_test() {
        assertThat(new S3SourceConfig().getCompression(), equalTo(CompressionOption.NONE));
    }
}
