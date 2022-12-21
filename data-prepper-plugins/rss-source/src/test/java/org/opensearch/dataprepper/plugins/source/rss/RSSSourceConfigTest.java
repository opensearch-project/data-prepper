package org.opensearch.dataprepper.plugins.source.rss;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.plugins.source.rss.RSSSourceConfig.DEFAULT_POLLING_FREQUENCY;

class RSSSourceConfigTest {

    @Test
    void test_default_pollingFrequency() {
        assertThat(new RSSSourceConfig().getPollingFrequency(), equalTo(DEFAULT_POLLING_FREQUENCY));
    }
}
