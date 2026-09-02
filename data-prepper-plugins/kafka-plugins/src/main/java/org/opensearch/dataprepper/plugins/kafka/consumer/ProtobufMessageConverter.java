/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.AnyProto;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DurationProto;
import com.google.protobuf.EmptyProto;
import com.google.protobuf.FieldMaskProto;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.StructProto;
import com.google.protobuf.TimestampProto;
import com.google.protobuf.WrappersProto;
import com.google.protobuf.util.JsonFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class ProtobufMessageConverter {
    private static final List<Descriptors.FileDescriptor> WELL_KNOWN_TYPE_FILES = List.of(
            AnyProto.getDescriptor(),
            DurationProto.getDescriptor(),
            EmptyProto.getDescriptor(),
            FieldMaskProto.getDescriptor(),
            StructProto.getDescriptor(),
            TimestampProto.getDescriptor(),
            WrappersProto.getDescriptor());

    private final ObjectMapper objectMapper;
    private final Map<Descriptors.Descriptor, JsonFormat.Printer> printerCache;

    ProtobufMessageConverter(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.printerCache = new HashMap<>();
    }

    Object convert(final Message message) throws IOException {
        final JsonFormat.Printer printer = printerCache.computeIfAbsent(
                message.getDescriptorForType(), this::createPrinter);
        try {
            return objectMapper.readValue(printer.print(message), Object.class);
        } catch (final InvalidProtocolBufferException | IllegalArgumentException e) {
            return convertMessageFields(message, printer);
        }
    }

    private JsonFormat.Printer createPrinter(final Descriptors.Descriptor descriptor) {
        final JsonFormat.TypeRegistry.Builder typeRegistryBuilder = JsonFormat.TypeRegistry.newBuilder();
        final Set<String> registeredFiles = new HashSet<>();
        addFileDescriptor(descriptor.getFile(), typeRegistryBuilder, registeredFiles);
        WELL_KNOWN_TYPE_FILES.forEach(fileDescriptor ->
                addFileDescriptor(fileDescriptor, typeRegistryBuilder, registeredFiles));
        return JsonFormat.printer().usingTypeRegistry(typeRegistryBuilder.build());
    }

    private void addFileDescriptor(
            final Descriptors.FileDescriptor fileDescriptor,
            final JsonFormat.TypeRegistry.Builder typeRegistryBuilder,
            final Set<String> registeredFiles) {
        if (!registeredFiles.add(fileDescriptor.getFullName())) {
            return;
        }

        fileDescriptor.getDependencies().forEach(dependency ->
                addFileDescriptor(dependency, typeRegistryBuilder, registeredFiles));
        fileDescriptor.getMessageTypes().forEach(descriptor ->
                addMessageDescriptor(descriptor, typeRegistryBuilder));
    }

    private void addMessageDescriptor(
            final Descriptors.Descriptor descriptor,
            final JsonFormat.TypeRegistry.Builder typeRegistryBuilder) {
        typeRegistryBuilder.add(descriptor);
        descriptor.getNestedTypes().forEach(nestedDescriptor ->
                addMessageDescriptor(nestedDescriptor, typeRegistryBuilder));
    }

    private Map<String, Object> convertMessageFields(
            final Message message,
            final JsonFormat.Printer printer) throws IOException {
        final Map<String, Object> convertedFields = new LinkedHashMap<>();
        for (final Map.Entry<Descriptors.FieldDescriptor, Object> field : message.getAllFields().entrySet()) {
            convertedFields.put(field.getKey().getJsonName(), convertField(field.getKey(), field.getValue(), printer));
        }
        return convertedFields;
    }

    private Object convertField(
            final Descriptors.FieldDescriptor fieldDescriptor,
            final Object value,
            final JsonFormat.Printer printer) throws IOException {
        if (fieldDescriptor.isMapField()) {
            return convertMapField((List<?>) value, printer);
        }
        if (fieldDescriptor.isRepeated()) {
            final List<Object> convertedValues = new ArrayList<>();
            for (final Object repeatedValue : (List<?>) value) {
                convertedValues.add(convertSingleValue(fieldDescriptor, repeatedValue, printer));
            }
            return convertedValues;
        }
        return convertSingleValue(fieldDescriptor, value, printer);
    }

    private Map<String, Object> convertMapField(
            final List<?> entries,
            final JsonFormat.Printer printer) throws IOException {
        final Map<String, Object> convertedMap = new LinkedHashMap<>();
        for (final Object entry : entries) {
            final Message entryMessage = (Message) entry;
            final Descriptors.FieldDescriptor keyField = entryMessage.getDescriptorForType().findFieldByName("key");
            final Descriptors.FieldDescriptor valueField = entryMessage.getDescriptorForType().findFieldByName("value");
            final Object key = convertSingleValue(keyField, entryMessage.getField(keyField), printer);
            final Object mapValue = convertSingleValue(valueField, entryMessage.getField(valueField), printer);
            convertedMap.put(String.valueOf(key), mapValue);
        }
        return convertedMap;
    }

    private Object convertSingleValue(
            final Descriptors.FieldDescriptor fieldDescriptor,
            final Object value,
            final JsonFormat.Printer printer) throws IOException {
        if (fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.UINT32
                || fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.FIXED32) {
            return Integer.toUnsignedLong((Integer) value);
        }
        if (fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.UINT64
                || fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.FIXED64) {
            return Long.toUnsignedString((Long) value);
        }

        switch (fieldDescriptor.getJavaType()) {
            case MESSAGE:
                return convert((Message) value, printer);
            case ENUM:
                final Descriptors.EnumValueDescriptor enumValue = (Descriptors.EnumValueDescriptor) value;
                return enumValue.getIndex() == -1 ? enumValue.getNumber() : enumValue.getName();
            case BYTE_STRING:
                return Base64.getEncoder().encodeToString(((ByteString) value).toByteArray());
            case LONG:
                return value.toString();
            case FLOAT:
            case DOUBLE:
                return convertFloatingPoint((Number) value);
            default:
                return value;
        }
    }

    private Object convert(final Message message, final JsonFormat.Printer printer) throws IOException {
        try {
            return objectMapper.readValue(printer.print(message), Object.class);
        } catch (final InvalidProtocolBufferException | IllegalArgumentException e) {
            return convertMessageFields(message, printer);
        }
    }

    private Object convertFloatingPoint(final Number value) {
        final double doubleValue = value.doubleValue();
        if (Double.isNaN(doubleValue)) {
            return "NaN";
        }
        if (doubleValue == Double.POSITIVE_INFINITY) {
            return "Infinity";
        }
        if (doubleValue == Double.NEGATIVE_INFINITY) {
            return "-Infinity";
        }
        return value;
    }
}
