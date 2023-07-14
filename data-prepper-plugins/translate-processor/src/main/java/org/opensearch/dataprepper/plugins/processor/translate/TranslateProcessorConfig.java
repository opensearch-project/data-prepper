/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.translate;


import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.stream.Stream;


public class TranslateProcessorConfig {

    @JsonProperty("file_path")
    private String filePath;

    @NotNull
    @JsonProperty("mappings")
    private List<MappingsParameterConfig> mappingsParameterConfigs;

    public String getFilePath() {
        return filePath;
    }

    public List<MappingsParameterConfig> getMappingsParameterConfigs() {
        return mappingsParameterConfigs;
    }

    @AssertTrue(message = "Either mappings or file_path option needs to be configured.")
    public boolean hasMappings() {
        return Stream.of(mappingsParameterConfigs, filePath).filter(n -> n != null).count() != 0;
    }

}
