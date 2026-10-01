/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerSourceConfig.DEFAULT_PARTITION_CREATION_WAIT;

class CrawlerSourceConfigTest {

    private final CrawlerSourceConfig sourceConfig = new CrawlerSourceConfig() {
        @Override
        public int getNumberOfWorkers() {
            return 1;
        }

        @Override
        public boolean isAcknowledgments() {
            return false;
        }
    };

    @Test
    void getPartitionCreationWait_whenNotOverridden_returnsFiveMinutes() {
        assertThat(DEFAULT_PARTITION_CREATION_WAIT, equalTo(Duration.ofMinutes(5)));
        assertThat(sourceConfig.getPartitionCreationWait(), equalTo(DEFAULT_PARTITION_CREATION_WAIT));
    }
}
