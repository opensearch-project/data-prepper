package org.opensearch.dataprepper.pipeline.parser.rule;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.jayway.jsonpath.Predicate;
import lombok.Data;
import lombok.NonNull;
import org.opensearch.dataprepper.pipeline.parser.model.TransformerValueDeserializer;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@Data
public class RuleTransformerModel {

    @NonNull
    private final String jsonPath;

    @NonNull
    private final String key;

    @NonNull
    private final Object value;

    @NonNull
    private final Set<Integer> validVersions;

    private final List<Predicate> predicates;

    @JsonCreator
    public RuleTransformerModel(@JsonProperty("json_path") final @NonNull String jsonPath,
                                @JsonProperty("key") final @NonNull String key,
                                @JsonProperty("value") @JsonDeserialize(using = TransformerValueDeserializer.class) final @NonNull Object value,
                                @JsonProperty("valid_versions") final @NonNull Set<Integer> validVersions,
                                @JsonProperty("predicates") final List<Predicate> predicates) {


        this.jsonPath = jsonPath;
        this.validVersions = validVersions;
        this.key = key;
        this.value = value;

        this.predicates = Objects.isNull(predicates) ? Collections.emptyList() : predicates;
    }
}