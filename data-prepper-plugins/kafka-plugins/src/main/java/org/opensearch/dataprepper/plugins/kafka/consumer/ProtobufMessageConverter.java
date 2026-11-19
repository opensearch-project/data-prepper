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
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.EmptyProto;
import com.google.protobuf.FieldMaskProto;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.StructProto;
import com.google.protobuf.TimestampProto;
import com.google.protobuf.WrappersProto;
import com.google.protobuf.util.JsonFormat;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Converts Protobuf messages into plain Java values following the ProtoJSON mapping,
 * except that 64-bit integer fields are emitted as numbers rather than strings so that
 * numeric conditions in a pipeline continue to work.
 */
class ProtobufMessageConverter {
    private static final List<Descriptors.FileDescriptor> WELL_KNOWN_TYPE_FILES = List.of(
            AnyProto.getDescriptor(),
            DurationProto.getDescriptor(),
            EmptyProto.getDescriptor(),
            FieldMaskProto.getDescriptor(),
            StructProto.getDescriptor(),
            TimestampProto.getDescriptor(),
            WrappersProto.getDescriptor());
    private static final String ANY_TYPE_NAME = "google.protobuf.Any";
    private static final String NULL_VALUE_ENUM_NAME = "google.protobuf.NullValue";
    private static final String ANY_TYPE_URL_FIELD = "type_url";
    private static final String ANY_VALUE_FIELD = "value";
    private static final String ANY_TYPE_KEY = "@type";
    private static final String ANY_VALUE_KEY = "value";
    private static final String WRAPPER_VALUE_FIELD = "value";
    private static final Set<String> WRAPPER_TYPE_NAMES = Set.of(
            "google.protobuf.DoubleValue",
            "google.protobuf.FloatValue",
            "google.protobuf.Int64Value",
            "google.protobuf.UInt64Value",
            "google.protobuf.Int32Value",
            "google.protobuf.UInt32Value",
            "google.protobuf.BoolValue",
            "google.protobuf.StringValue",
            "google.protobuf.BytesValue");
    private static final Set<String> JSON_PRINTED_TYPE_NAMES = Set.of(
            "google.protobuf.Timestamp",
            "google.protobuf.Duration",
            "google.protobuf.FieldMask",
            "google.protobuf.Struct",
            "google.protobuf.Value",
            "google.protobuf.ListValue",
            "google.protobuf.Empty");

    private final ObjectMapper objectMapper;
    private final Map<Descriptors.Descriptor, TypeContext> typeContextCache;

    ProtobufMessageConverter(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.typeContextCache = new ConcurrentHashMap<>();
    }

    Object convert(final Message message) throws IOException {
        final TypeContext typeContext = typeContextCache.computeIfAbsent(
                message.getDescriptorForType(), this::createTypeContext);
        return convertMessage(message, typeContext);
    }

    private TypeContext createTypeContext(final Descriptors.Descriptor descriptor) {
        final JsonFormat.TypeRegistry.Builder typeRegistryBuilder = JsonFormat.TypeRegistry.newBuilder();
        final Set<String> registeredFiles = new HashSet<>();
        addFileDescriptor(descriptor.getFile(), typeRegistryBuilder, registeredFiles);
        WELL_KNOWN_TYPE_FILES.forEach(fileDescriptor ->
                addFileDescriptor(fileDescriptor, typeRegistryBuilder, registeredFiles));
        final JsonFormat.TypeRegistry typeRegistry = typeRegistryBuilder.build();
        return new TypeContext(typeRegistry, JsonFormat.printer().usingTypeRegistry(typeRegistry));
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

    private Object convertMessage(final Message message, final TypeContext typeContext) throws IOException {
        final Descriptors.Descriptor descriptor = message.getDescriptorForType();
        final String typeName = descriptor.getFullName();
        if (WRAPPER_TYPE_NAMES.contains(typeName)) {
            final Descriptors.FieldDescriptor valueField = descriptor.findFieldByName(WRAPPER_VALUE_FIELD);
            return convertSingleValue(valueField, message.getField(valueField), typeContext);
        }
        if (ANY_TYPE_NAME.equals(typeName)) {
            return convertAny(message, typeContext);
        }
        if (JSON_PRINTED_TYPE_NAMES.contains(typeName)) {
            try {
                return objectMapper.readValue(typeContext.printer.print(message), Object.class);
            } catch (final InvalidProtocolBufferException | IllegalArgumentException e) {
                return convertMessageFields(message, typeContext);
            }
        }
        return convertMessageFields(message, typeContext);
    }

    private Object convertAny(final Message message, final TypeContext typeContext) throws IOException {
        final Descriptors.Descriptor anyDescriptor = message.getDescriptorForType();
        final String typeUrl = (String) message.getField(anyDescriptor.findFieldByName(ANY_TYPE_URL_FIELD));
        final Descriptors.Descriptor packedDescriptor = typeContext.typeRegistry.find(typeNameFromTypeUrl(typeUrl));
        if (packedDescriptor == null) {
            return convertMessageFields(message, typeContext);
        }

        final ByteString payload = (ByteString) message.getField(anyDescriptor.findFieldByName(ANY_VALUE_FIELD));
        final Message packedMessage;
        try {
            packedMessage = DynamicMessage.parseFrom(packedDescriptor, payload);
        } catch (final InvalidProtocolBufferException e) {
            return convertMessageFields(message, typeContext);
        }

        final Object packedValue = convertMessage(packedMessage, typeContext);
        final Map<String, Object> convertedAny = new LinkedHashMap<>();
        convertedAny.put(ANY_TYPE_KEY, typeUrl);
        if (packedValue instanceof Map && !hasSpecialJsonMapping(packedDescriptor)) {
            ((Map<?, ?>) packedValue).forEach((key, value) -> convertedAny.put(String.valueOf(key), value));
        } else {
            convertedAny.put(ANY_VALUE_KEY, packedValue);
        }
        return convertedAny;
    }

    private String typeNameFromTypeUrl(final String typeUrl) {
        return typeUrl.substring(typeUrl.lastIndexOf('/') + 1);
    }

    private boolean hasSpecialJsonMapping(final Descriptors.Descriptor descriptor) {
        final String typeName = descriptor.getFullName();
        return WRAPPER_TYPE_NAMES.contains(typeName)
                || JSON_PRINTED_TYPE_NAMES.contains(typeName)
                || ANY_TYPE_NAME.equals(typeName);
    }

    private Map<String, Object> convertMessageFields(
            final Message message,
            final TypeContext typeContext) throws IOException {
        final Map<String, Object> convertedFields = new LinkedHashMap<>();
        for (final Map.Entry<Descriptors.FieldDescriptor, Object> field : message.getAllFields().entrySet()) {
            convertedFields.put(field.getKey().getJsonName(), convertField(field.getKey(), field.getValue(), typeContext));
        }
        return convertedFields;
    }

    private Object convertField(
            final Descriptors.FieldDescriptor fieldDescriptor,
            final Object value,
            final TypeContext typeContext) throws IOException {
        if (fieldDescriptor.isMapField()) {
            return convertMapField((List<?>) value, typeContext);
        }
        if (fieldDescriptor.isRepeated()) {
            final List<Object> convertedValues = new ArrayList<>();
            for (final Object repeatedValue : (List<?>) value) {
                convertedValues.add(convertSingleValue(fieldDescriptor, repeatedValue, typeContext));
            }
            return convertedValues;
        }
        return convertSingleValue(fieldDescriptor, value, typeContext);
    }

    private Map<String, Object> convertMapField(
            final List<?> entries,
            final TypeContext typeContext) throws IOException {
        final Map<String, Object> convertedMap = new LinkedHashMap<>();
        for (final Object entry : entries) {
            final Message entryMessage = (Message) entry;
            final Descriptors.FieldDescriptor keyField = entryMessage.getDescriptorForType().findFieldByName("key");
            final Descriptors.FieldDescriptor valueField = entryMessage.getDescriptorForType().findFieldByName("value");
            final Object key = convertSingleValue(keyField, entryMessage.getField(keyField), typeContext);
            final Object mapValue = convertSingleValue(valueField, entryMessage.getField(valueField), typeContext);
            convertedMap.put(String.valueOf(key), mapValue);
        }
        return convertedMap;
    }

    private Object convertSingleValue(
            final Descriptors.FieldDescriptor fieldDescriptor,
            final Object value,
            final TypeContext typeContext) throws IOException {
        switch (fieldDescriptor.getType()) {
            case UINT32:
            case FIXED32:
                return Integer.toUnsignedLong((Integer) value);
            case UINT64:
            case FIXED64:
                return convertUnsignedLong((Long) value);
            default:
                break;
        }

        switch (fieldDescriptor.getJavaType()) {
            case MESSAGE:
                return convertMessage((Message) value, typeContext);
            case ENUM:
                final Descriptors.EnumValueDescriptor enumValue = (Descriptors.EnumValueDescriptor) value;
                if (NULL_VALUE_ENUM_NAME.equals(enumValue.getType().getFullName())) {
                    return null;
                }
                return enumValue.getIndex() == -1 ? enumValue.getNumber() : enumValue.getName();
            case BYTE_STRING:
                return Base64.getEncoder().encodeToString(((ByteString) value).toByteArray());
            case FLOAT:
            case DOUBLE:
                return convertFloatingPoint((Number) value);
            default:
                return value;
        }
    }

    private Object convertUnsignedLong(final Long value) {
        if (value >= 0) {
            return value;
        }
        return new BigInteger(Long.toUnsignedString(value));
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

    private static class TypeContext {
        private final JsonFormat.TypeRegistry typeRegistry;
        private final JsonFormat.Printer printer;

        private TypeContext(final JsonFormat.TypeRegistry typeRegistry, final JsonFormat.Printer printer) {
            this.typeRegistry = typeRegistry;
            this.printer = printer;
        }
    }
}
