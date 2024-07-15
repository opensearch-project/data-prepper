/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation for an {@link EventKey} used in a Data Prepper pipeline configuration.
 * <p>
 * Unless you need all actions on a configuration, you should use this annotation to
 * provide the most appropriate validation.
 *
 * @since 2.9
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface EventKeyConfiguration {
    /**
     * Defines the {@link EventKeyFactory.EventAction}s to use when creating the {@link EventKey}
     * for the configuration.
     *
     * @return The desired event actions.
     * @since 2.9
     */
    EventKeyFactory.EventAction[] value();
}
