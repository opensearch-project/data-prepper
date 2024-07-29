/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.translate;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


public class TranslateProcessorConfig {

    @JsonProperty("file")
    @JsonPropertyDescription("Points to the file that contains mapping configurations. For more information, see [file](#file).")
    @Valid
    private FileParameterConfig fileParameterConfig;

    @JsonProperty("mappings")
    @JsonPropertyDescription("Defines inline mappings. For more information, see [mappings](#mappings).")
    @Valid
    private List<MappingsParameterConfig> mappingsParameterConfigs = new ArrayList<>();

    @JsonIgnore
    private List<MappingsParameterConfig> fileMappingsConfigs;

    @JsonIgnore
    private List<MappingsParameterConfig> combinedMappingsConfigs;

    public FileParameterConfig getFileParameterConfig() {
        return fileParameterConfig;
    }

    public List<MappingsParameterConfig> getMappingsParameterConfigs() {
        return mappingsParameterConfigs;
    }

    public List<MappingsParameterConfig> getCombinedMappingsConfigs() {
        return combinedMappingsConfigs;
    }

    @AssertTrue(message = "Please ensure that at least one of the options, either \"mappings\" or \"file_path\", is properly configured.")
    public boolean hasMappings() {
        isFileValid();
        MappingsHandler handler = new MappingsHandler();
        combinedMappingsConfigs = handler.getCombinedMappings(mappingsParameterConfigs, fileMappingsConfigs);
        return Objects.nonNull(combinedMappingsConfigs);
    }

    @AssertTrue(message = "\"mappings\" option should not be empty.")
    public boolean isMappingsValid() {
        return Objects.nonNull(mappingsParameterConfigs);
    }

    @AssertTrue(message = "The file specified in the \"file_path\" option is not properly configured.")
    public boolean isFileValid() {
        if (fileParameterConfig == null) {
            return true;
        }
        fileMappingsConfigs = fileParameterConfig.getFileMappings();
        return Objects.nonNull(fileMappingsConfigs);
    }

}
