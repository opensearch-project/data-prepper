/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.documentdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class DocumentDBServiceTest {
    private static final long EPOCH_MILLIS = 1761000000000L;

    @ParameterizedTest
    @ValueSource(strings = {"annuitycontract", "annuitycontract/", "/annuitycontract", "/annuitycontract/", "annuitycontract//", " annuitycontract/ "})
    void buildS3PathPrefix_sameLayout_withOrWithoutSurroundingSlashes(final String userPrefix) {
        assertThat(DocumentDBService.buildS3PathPrefix(userPrefix, "pipeline-a", EPOCH_MILLIS),
                equalTo("annuitycontract/pipeline-a/" + EPOCH_MILLIS + "/"));
        assertThat(DocumentDBService.buildS3PathPrefix(userPrefix, null, EPOCH_MILLIS),
                equalTo("annuitycontract/" + EPOCH_MILLIS + "/"));
    }

    @Test
    void buildS3PathPrefix_nestedPrefix_collapsesRepeatedDelimiters() {
        assertThat(DocumentDBService.buildS3PathPrefix("a//b/c/", null, EPOCH_MILLIS),
                equalTo("a/b/c/" + EPOCH_MILLIS + "/"));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "/", "///"})
    void buildS3PathPrefix_noUsablePrefix_startsAtBucketRoot(final String userPrefix) {
        assertThat(DocumentDBService.buildS3PathPrefix(userPrefix, "pipeline-a", EPOCH_MILLIS),
                equalTo("pipeline-a/" + EPOCH_MILLIS + "/"));
        assertThat(DocumentDBService.buildS3PathPrefix(userPrefix, null, EPOCH_MILLIS),
                equalTo(EPOCH_MILLIS + "/"));
    }
}
