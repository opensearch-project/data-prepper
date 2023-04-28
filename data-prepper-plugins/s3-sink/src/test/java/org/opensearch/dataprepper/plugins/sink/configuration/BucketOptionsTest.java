/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.configuration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Test;

class BucketOptionsTest {

    @Test
    void get_bucket_name_test() {
        assertThat(new BucketOptions().getBucketName(), equalTo(null));
    }

    @Test
    void get_object_key_test() {
        assertThat("Object key is not an instance of ObjectKeyOptions",
                new BucketOptions().getObjectKeyOptions() instanceof ObjectKeyOptions);
    }
}