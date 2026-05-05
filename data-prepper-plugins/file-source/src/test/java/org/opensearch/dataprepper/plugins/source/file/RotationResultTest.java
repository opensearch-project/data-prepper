/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.file;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ExtendWith(MockitoExtension.class)
class RotationResultTest {

    @Mock
    private FileIdentity fileIdentity;

    @Test
    void noRotationConstantHasCorrectType() {
        assertThat(RotationResult.NO_ROTATION.getRotationType(), equalTo(RotationType.NO_ROTATION));
    }

    @Test
    void noRotationConstantHasNullNewFileIdentity() {
        assertThat(RotationResult.NO_ROTATION.getNewFileIdentity(), nullValue());
    }

    @Test
    void deletedConstantHasCorrectType() {
        assertThat(RotationResult.DELETED.getRotationType(), equalTo(RotationType.DELETED));
    }

    @Test
    void deletedConstantHasNullNewFileIdentity() {
        assertThat(RotationResult.DELETED.getNewFileIdentity(), nullValue());
    }

    @Test
    void constructorSetsRotationType() {
        final RotationResult result = new RotationResult(RotationType.CREATE_RENAME, fileIdentity);

        assertThat(result.getRotationType(), equalTo(RotationType.CREATE_RENAME));
    }

    @Test
    void constructorSetsNewFileIdentity() {
        final RotationResult result = new RotationResult(RotationType.COPYTRUNCATE, fileIdentity);

        assertThat(result.getNewFileIdentity(), equalTo(fileIdentity));
    }

    @Test
    void constructorAllowsNullNewFileIdentity() {
        final RotationResult result = new RotationResult(RotationType.NO_ROTATION, null);

        assertThat(result.getNewFileIdentity(), nullValue());
    }

    @Test
    void toStringContainsRotationType() {
        final RotationResult result = new RotationResult(RotationType.CREATE_RENAME, fileIdentity);

        assertThat(result.toString(), notNullValue());
        assertThat(result.toString(), containsString("CREATE_RENAME"));
    }

    @Test
    void toStringContainsNewIdentityWhenPresent() {
        final RotationResult result = new RotationResult(RotationType.CREATE_RENAME, fileIdentity);

        assertThat(result.toString(), containsString("newIdentity="));
    }

    @Test
    void toStringOmitsNewIdentityWhenNull() {
        final RotationResult result = new RotationResult(RotationType.NO_ROTATION, null);

        final String str = result.toString();
        assertThat(str.contains("newIdentity="), equalTo(false));
    }
}
