package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.Test;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class S3ScanSchedulingOptionsTest {
    private final ObjectMapper objectMapper =
            new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS)).registerModule(new JavaTimeModule());

    @Test
    public void s3_scan_bucket_option_yaml_configuration_test() throws JsonProcessingException {

        final String schedulingOptionsYaml = "rate: \"PT1H\" \ncount: 2 \n";
        final S3ScanSchedulingOptions s3ScanSchedulingOptions = objectMapper.readValue(schedulingOptionsYaml, S3ScanSchedulingOptions.class);
        assertThat(s3ScanSchedulingOptions.getCount(), equalTo(2));
        assertThat(s3ScanSchedulingOptions.getRate(), equalTo(Duration.ofHours(1)));
    }
}