/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.deser.BeanDeserializerBuilder;
import com.fasterxml.jackson.databind.deser.SettableBeanProperty;
import com.fasterxml.jackson.databind.introspect.AnnotatedField;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import org.opensearch.dataprepper.model.annotations.UsesDataPrepperPlugin;
import org.opensearch.dataprepper.model.configuration.PluginModel;

import java.util.Iterator;
import java.util.List;

class DataPrepperPluginBeanDeserializerModifier extends BeanDeserializerModifier {

    @Override
    public BeanDeserializerBuilder updateBuilder(
            final DeserializationConfig config,
            final BeanDescription beanDesc,
            final BeanDeserializerBuilder builder) {

        final List<BeanPropertyDefinition> properties = beanDesc.findProperties();

        for (final BeanPropertyDefinition propertyDef : properties) {
            final UsesDataPrepperPlugin annotation = findAnnotation(propertyDef);
            if (annotation == null) {
                continue;
            }

            if (PluginModel.class.isAssignableFrom(propertyDef.getRawPrimaryType())) {
                continue;
            }

            final Class<?> pluginType = annotation.pluginType();
            final NestedPluginDeserializer deserializer = new NestedPluginDeserializer(pluginType);

            final Iterator<SettableBeanProperty> propertyIterator = builder.getProperties();
            while (propertyIterator.hasNext()) {
                final SettableBeanProperty property = propertyIterator.next();
                if (property.getName().equals(propertyDef.getName())) {
                    final SettableBeanProperty updatedProperty = property.withValueDeserializer(deserializer);
                    builder.addOrReplaceProperty(updatedProperty, true);
                    break;
                }
            }
        }

        return builder;
    }

    private UsesDataPrepperPlugin findAnnotation(final BeanPropertyDefinition propertyDef) {
        final AnnotatedField field = propertyDef.getField();
        if (field != null) {
            final UsesDataPrepperPlugin annotation = field.getAnnotation(UsesDataPrepperPlugin.class);
            if (annotation != null) {
                return annotation;
            }
        }

        final AnnotatedMember mutator = propertyDef.getMutator();
        if (mutator != null) {
            final UsesDataPrepperPlugin annotation = mutator.getAnnotation(UsesDataPrepperPlugin.class);
            if (annotation != null) {
                return annotation;
            }
        }

        return null;
    }
}
