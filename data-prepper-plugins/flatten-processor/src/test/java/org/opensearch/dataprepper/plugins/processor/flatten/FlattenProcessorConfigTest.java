/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.flatten;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.opensearch.dataprepper.model.event.FieldConflictStrategy;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FlattenProcessorConfigTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void testDefaultConfig() {
        final FlattenProcessorConfig flattenProcessorConfig = new FlattenProcessorConfig();

        assertThat(flattenProcessorConfig.getSource(), equalTo(null));
        assertThat(flattenProcessorConfig.getTarget(), equalTo(null));
        assertThat(flattenProcessorConfig.isRemoveListIndices(), equalTo(false));
        assertThat(flattenProcessorConfig.isRemoveBrackets(), equalTo(false));
        assertThat(flattenProcessorConfig.getFlattenWhen(), equalTo(null));
        assertThat(flattenProcessorConfig.getTagsOnFailure(), equalTo(null));
        assertThat(flattenProcessorConfig.getExcludeKeys(), equalTo(List.of()));
        assertThat(flattenProcessorConfig.getTargetConflictStrategy(), equalTo(FieldConflictStrategy.OVERWRITE));
    }

    @ParameterizedTest
    @EnumSource(value = FieldConflictStrategy.class, names = "UNKNOWN", mode = EnumSource.Mode.EXCLUDE)
    void testTargetConflictStrategyDeserialization(final FieldConflictStrategy strategy) {
        final Map<String, Object> map = Map.of(
                "source", "src",
                "target", "tgt",
                "target_conflict_strategy", strategy.getOptionName()
        );
        final FlattenProcessorConfig config = OBJECT_MAPPER.convertValue(map, FlattenProcessorConfig.class);
        assertThat(config.getTargetConflictStrategy(), equalTo(strategy));
    }

    @Test
    void testTargetConflictStrategy_invalidString_throwsException() {
        final Map<String, Object> map = Map.of(
                "source", "src",
                "target", "tgt",
                "target_conflict_strategy", "invalid_strategy"
        );
        assertThrows(IllegalArgumentException.class, () -> OBJECT_MAPPER.convertValue(map, FlattenProcessorConfig.class));
    }

    @Test
    void testTargetConflictStrategy_unknownSentinel_isRejected() {
        final Map<String, Object> map = Map.of(
                "source", "src",
                "target", "tgt",
                "target_conflict_strategy", "unknown"
        );
        assertThrows(IllegalArgumentException.class, () -> OBJECT_MAPPER.convertValue(map, FlattenProcessorConfig.class));
    }
}
