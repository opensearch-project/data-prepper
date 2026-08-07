/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.typeconverter;

/**
 * Interface for arguments passed to the {@link TypeConverter}
 *
 * @since 1.2
 */
public interface ConverterArguments {
    int getScale();
}
