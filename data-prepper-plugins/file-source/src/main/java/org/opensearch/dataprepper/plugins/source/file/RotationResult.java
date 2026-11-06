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

import java.util.Objects;

public final class RotationResult {

    static final RotationResult NO_ROTATION = new RotationResult(RotationType.NO_ROTATION, null);
    static final RotationResult DELETED = new RotationResult(RotationType.DELETED, null);

    private final RotationType rotationType;
    private final FileIdentity newFileIdentity;

    RotationResult(final RotationType rotationType, final FileIdentity newFileIdentity) {
        this.rotationType = Objects.requireNonNull(rotationType, "rotationType must not be null");
        this.newFileIdentity = newFileIdentity;
    }

    public RotationType getRotationType() {
        return rotationType;
    }

    public FileIdentity getNewFileIdentity() {
        return newFileIdentity;
    }

    @Override
    public String toString() {
        return "RotationResult{type=" + rotationType +
                (newFileIdentity != null ? ", newIdentity=" + newFileIdentity : "") + "}";
    }
}
