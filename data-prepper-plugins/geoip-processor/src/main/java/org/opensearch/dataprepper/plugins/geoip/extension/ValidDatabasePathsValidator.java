/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.extension;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import org.opensearch.dataprepper.plugins.geoip.extension.databasedownload.DBSourceOptions;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ValidDatabasePathsValidator implements ConstraintValidator<ValidDatabasePaths, MaxMindDatabaseConfig> {

    @Override
    public boolean isValid(final MaxMindDatabaseConfig value, final ConstraintValidatorContext context) {
        if (value == null) {
            return true;
        }

        final List<String> validationErrors = value.getDatabasePathValidationErrors();
        if (validationErrors.isEmpty()) {
            return true;
        }

        context.disableDefaultConstraintViolation();
        validationErrors.forEach(validationError ->
                context.buildConstraintViolationWithTemplate(validationError)
                        .addConstraintViolation());
        return false;
    }

    static List<String> getDatabasePathValidationErrors(final Map<String, String> databasePaths) {
        final List<PathValidationResult> validationResults = new ArrayList<>();
        final List<String> validationErrors = new ArrayList<>();

        databasePaths.forEach((databaseName, databasePath) -> {
            final DBSourceOptions sourceType = getDatabasePathSourceType(databasePath);
            if (sourceType == null) {
                validationErrors.add(getInvalidDatabasePathMessage(databaseName, databasePath));
            }
            validationResults.add(new PathValidationResult(databasePath, sourceType));
        });

        if (validationErrors.isEmpty()) {
            validationErrors.addAll(getMixedSourceTypeMessages(validationResults));
        }

        return validationErrors;
    }

    private static DBSourceOptions getDatabasePathSourceType(final String databasePath) {
        if (databasePath == null || databasePath.trim().isEmpty()) {
            return null;
        }
        if (DatabaseSourceIdentification.isFilePath(databasePath)) {
            return DBSourceOptions.PATH;
        }
        if (DatabaseSourceIdentification.isCDNEndpoint(databasePath)) {
            return DBSourceOptions.HTTP_MANIFEST;
        }
        if (DatabaseSourceIdentification.isURL(databasePath)) {
            return DBSourceOptions.URL;
        }
        if (DatabaseSourceIdentification.isS3Uri(databasePath)) {
            return DBSourceOptions.S3;
        }
        return null;
    }

    private static List<String> getMixedSourceTypeMessages(final List<PathValidationResult> validationResults) {
        final Set<DBSourceOptions> sourceTypes = new LinkedHashSet<>();
        validationResults.forEach(validationResult -> sourceTypes.add(validationResult.sourceType));

        if (sourceTypes.size() <= 1) {
            return List.of();
        }

        final List<String> validationErrors = new ArrayList<>();
        validationResults.forEach(validationResult ->
                validationErrors.add("Mixed database path source types are not supported. Found "
                        + getSourceTypeDescription(validationResult.sourceType) + ": " + validationResult.databasePath));
        return validationErrors;
    }

    private static String getInvalidDatabasePathMessage(final String databaseName, final String databasePath) {
        if (databasePath == null) {
            return "Database path must not be null: " + databaseName;
        }
        if (databasePath.trim().isEmpty()) {
            return "Database path must not be blank: " + databaseName;
        }

        final File databaseFile = new File(databasePath);
        if (databaseFile.exists()) {
            if (databaseFile.isDirectory()) {
                return "Directory provided, but a file is required: " + databasePath;
            }
            return "Path is not a regular file: " + databasePath;
        }

        final String uriScheme = getUriScheme(databasePath);
        if (uriScheme == null) {
            return "Path does not exist: " + databasePath;
        }
        if (uriScheme.equalsIgnoreCase("http") || uriScheme.equalsIgnoreCase("https")) {
            return "HTTP endpoint must be a MaxMind download URL or manifest endpoint: " + databasePath;
        }
        return "Unsupported URI scheme for database path: " + databasePath;
    }

    private static String getUriScheme(final String databasePath) {
        try {
            return new URI(databasePath).getScheme();
        } catch (final URISyntaxException e) {
            return null;
        }
    }

    private static String getSourceTypeDescription(final DBSourceOptions sourceType) {
        switch (sourceType) {
            case PATH:
                return "local file path";
            case HTTP_MANIFEST:
                return "HTTP manifest endpoint";
            case URL:
                return "MaxMind download URL";
            case S3:
                return "S3 URI";
            default:
                throw new IllegalArgumentException("Unsupported database path source type: " + sourceType);
        }
    }

    private static class PathValidationResult {
        private final String databasePath;
        private final DBSourceOptions sourceType;

        private PathValidationResult(final String databasePath, final DBSourceOptions sourceType) {
            this.databasePath = databasePath;
            this.sourceType = sourceType;
        }
    }
}
