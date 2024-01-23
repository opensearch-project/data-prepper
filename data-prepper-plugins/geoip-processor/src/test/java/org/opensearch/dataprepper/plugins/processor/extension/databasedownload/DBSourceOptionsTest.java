/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension.databasedownload;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockitoExtension.class)
class DBSourceOptionsTest {

    @Test
    void notNull_test() {
        assertNotNull(DBSourceOptions.S3);
    }

    @Test
    void fromOptionValue_test() {
        DBSourceOptions downloadSourceOptions = DBSourceOptions.fromOptionValue("s3");
        assertNotNull(downloadSourceOptions);
        assertThat(downloadSourceOptions.toString(), equalTo("S3"));
    }
}