/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.translate;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;

import java.util.List;

public class FileMappingsRef {
    @JsonProperty("mappings")
    @Valid
    private List<MappingsParameterConfig> fileMappingConfigs;

    public List<MappingsParameterConfig> getFileMappingConfigs(){
        return  fileMappingConfigs;
    }

}
