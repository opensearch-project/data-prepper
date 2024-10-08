/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.translate;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.List;

public class FileParameterConfig {

    @JsonProperty("name")
    @JsonPropertyDescription("The full path to a local file or key name for an S3 object.")
    @NotNull
    private String fileName;

    @JsonProperty("aws")
    @JsonPropertyDescription("The AWS configuration when the file is an S3 object.")
    @Valid
    private S3ObjectConfig awsConfig;

    public String getFileName(){
        return  fileName;
    }

    public S3ObjectConfig getAwsConfig(){
        return awsConfig;
    }

    public List<MappingsParameterConfig> getFileMappings() {
        MappingsHandler handler = new MappingsHandler();

        if (this.awsConfig != null) {
            return handler.getS3FileMappings(this.awsConfig, this.fileName);
        } else{
            return handler.getMappingsFromFilePath(this.fileName);
        }
    }

}
