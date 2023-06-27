package org.opensearch.dataprepper.plugins.sink.configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.sink.config.CwlSinkConfig;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class CwlSinkConfigTest {
    @Test
    void check_null_auth_config_test() {
        assertThat(new CwlSinkConfig().getAwsConfig(), equalTo(null));
    }

    @Test
    void check_default_buffer_type_test() {
        assertThat(new CwlSinkConfig().getBufferType(), equalTo(CwlSinkConfig.DEFAULT_BUFFER_TYPE));
    }

    @Test
    void check_null_log_group_test() {
        assertThat(new CwlSinkConfig().getLogGroup(), equalTo(null));
    }
    @Test
    void check_null_log_stream_test() {
        assertThat(new CwlSinkConfig().getLogStream(), equalTo(null));
    }

}
