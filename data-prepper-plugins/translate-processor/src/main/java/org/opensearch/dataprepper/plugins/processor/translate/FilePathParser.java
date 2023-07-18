package org.opensearch.dataprepper.plugins.processor.translate;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class FilePathParser {
    @JsonProperty("mappings")
    @Valid
    private List<MappingsParameterConfig> fileMappingConfigs;

    public Optional<List<MappingsParameterConfig>> getCombinedMappings(List<MappingsParameterConfig> mappingConfigs) {
        if(Objects.isNull(mappingConfigs) || mappingConfigs.isEmpty()){
            return Optional.ofNullable(fileMappingConfigs);
        }
        try{
            for (MappingsParameterConfig fileMappingConfig : fileMappingConfigs) {
                boolean isDuplicateSource = false;
                for (MappingsParameterConfig mappingConfig : mappingConfigs) {
                    if (mappingConfig.getSource().equals(fileMappingConfig.getSource())) {
                        isDuplicateSource = true;
                        combineTargets(fileMappingConfig, mappingConfig);
                    }
                }
                if (!isDuplicateSource) {
                    mappingConfigs.add(fileMappingConfig);
                }
            }
            return Optional.of(mappingConfigs);
        } catch (Exception ex){
            return Optional.empty();
        }
    }

    private void combineTargets(MappingsParameterConfig filePathMapping, MappingsParameterConfig mappingConfig) {
        if(Objects.isNull(mappingConfig)){
            return;
        }
        List<TargetsParameterConfig> fileTargetConfigs = filePathMapping.getTargetsParameterConfigs();
        List<TargetsParameterConfig> mappingsTargetConfigs = mappingConfig.getTargetsParameterConfigs();
        List<TargetsParameterConfig> combinedTargetConfigs = new ArrayList<>(mappingsTargetConfigs);

        for (TargetsParameterConfig fileTargetConfig : fileTargetConfigs) {
            if (!isTargetPresent(fileTargetConfig, combinedTargetConfigs)) {
                combinedTargetConfigs.add(fileTargetConfig);
            }
        }
        mappingConfig.setTargetsParameterConfigs(combinedTargetConfigs);
    }

    private boolean isTargetPresent(TargetsParameterConfig fileTargetConfig, List<TargetsParameterConfig> combinedTargetConfigs){
        String fileTarget = fileTargetConfig.getTarget();
        return combinedTargetConfigs.stream().anyMatch(targetConfig -> fileTarget.equals(targetConfig.getTarget()));
    }

}
