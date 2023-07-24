/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.translate;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.util.List;

public class FileParameterConfig {

    @JsonProperty("name")
    @NotNull
    private String fileName;

    @JsonProperty("aws")
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
        } else {
            File localFile = new File(this.fileName);
            return handler.getFileMappings(localFile);
        }
    }


}
