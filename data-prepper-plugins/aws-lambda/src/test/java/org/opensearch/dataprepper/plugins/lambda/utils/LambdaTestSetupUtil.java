package org.opensearch.dataprepper.plugins.lambda.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.io.InputStream;
import org.opensearch.dataprepper.plugins.lambda.processor.LambdaProcessorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LambdaTestSetupUtil {

  private static final Logger log = LoggerFactory.getLogger(LambdaTestSetupUtil.class);

  public static ObjectMapper getObjectMapper() {
    return new ObjectMapper(
        new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS)).registerModule(
        new JavaTimeModule());
  }

  private static InputStream getResourceAsStream(String resourceName) {
    InputStream inputStream = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream(resourceName);
    if (inputStream == null) {
      inputStream = LambdaTestSetupUtil.class.getResourceAsStream("/" + resourceName);
    }
    return inputStream;
  }

  public static LambdaProcessorConfig createLambdaConfigurationFromYaml(String fileName) {
    ObjectMapper objectMapper = getObjectMapper();
    try (InputStream inputStream = getResourceAsStream(fileName)) {
      return objectMapper.readValue(inputStream, LambdaProcessorConfig.class);
    } catch (IOException ex) {
      log.error("Failed to parse pipeline Yaml", ex);
      throw new RuntimeException(ex);
    }
  }

}
