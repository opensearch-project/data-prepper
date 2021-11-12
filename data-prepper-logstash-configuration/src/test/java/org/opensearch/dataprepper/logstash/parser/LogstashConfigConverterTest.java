package org.opensearch.dataprepper.logstash.parser;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class LogstashConfigConverterTest {
    private LogstashConfigConverter logstashConfigConverter;

    @BeforeEach
    void createObjectUnderTest() {
        logstashConfigConverter = new LogstashConfigConverter();
    }

    @Test
    void convertLogstashConfigurationToPipeline() throws IOException {

        String outputDirectory = "build/resources/test/org/opensearch/dataprepper/logstash/parser/";
        String confPath = "src/test/resources/org/opensearch/dataprepper/logstash/parser/logstash.conf";

        String actualYamlPath =  logstashConfigConverter.convertLogstashConfigurationToPipeline(confPath, outputDirectory);
        String expectedYamlPath = "build/resources/test/org/opensearch/dataprepper/logstash/parser/logstash.yaml";

        assertThat(actualYamlPath, equalTo(expectedYamlPath));
    }
}