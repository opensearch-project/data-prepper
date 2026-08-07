/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.gradle.end_to_end;

import org.gradle.api.DefaultTask;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;

/**
 * A task which can provide a Docker image to use for an end-to-end test.
 */
public abstract class DockerProviderTask extends DefaultTask {
    /**
     * The Docker image with both the name and tag in the standard string
     * format - <i>my-image:mytag</i>
     *
     * @return The Docker image
     */
    @Input
    abstract Property<String> getImageId();
}
