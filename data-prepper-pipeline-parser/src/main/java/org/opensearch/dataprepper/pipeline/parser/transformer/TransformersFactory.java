/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser.transformer;

import org.opensearch.dataprepper.pipeline.parser.rule.RuleStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Stream;

public class TransformersFactory {

    private static final Logger LOG = LoggerFactory.getLogger(TransformersFactory.class);
    private static final String TEMPLATES_PATH = "org/opensearch/dataprepper/transforms/templates/";
    private static final String RULES_PATH = "org/opensearch/dataprepper/transforms/rules/";
    private final String TEMPLATE_FILE_NAME_PATTERN = "-template.yaml";
    private final String RULE_FILE_NAME_PATTERN = "-rule.yaml";

    public TransformersFactory(){
    }


    public InputStream getPluginTemplateFileStream(String pluginName) {
        if (pluginName == null || pluginName.isEmpty()) {
            throw new RuntimeException("Transformation plugin not found");
        }

        // Construct the expected file name
        String templateFileName = pluginName + TEMPLATE_FILE_NAME_PATTERN;

        // Use the ClassLoader to find the template file on the classpath
        ClassLoader classLoader = getClass().getClassLoader();
        URL templateURL = classLoader.getResource(TEMPLATES_PATH + templateFileName);

        if (templateURL == null) {
            throw new RuntimeException("Template file not found for plugin: " + pluginName);
        }

        try {
            // Convert the URL to a URI, then to a Path to read the file
            Path templatePath;
            try {
                templatePath = Paths.get(templateURL.toURI());
            } catch (FileSystemNotFoundException e) {
                // Handle the case where the file system is not accessible (e.g., in a JAR)
                FileSystem fileSystem = FileSystems.newFileSystem(templateURL.toURI(), Collections.emptyMap());
                templatePath = fileSystem.getPath(TEMPLATES_PATH + templateFileName);
            }

            // Return an InputStream for the found file
            return Files.newInputStream(templatePath);

        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException("Failed to load template file for plugin: " + pluginName, e);
        }
    }
    public Collection<RuleStream> loadRules() {
        List<RuleStream> ruleStreams = new ArrayList<>();
        ClassLoader classLoader = getClass().getClassLoader();

        try {
            // Use ClassLoader to find all resources that match the RULES_PATH pattern
            Enumeration<URL> rulesURLs = classLoader.getResources(RULES_PATH);

            while (rulesURLs.hasMoreElements()) {
                URL rulesURL = rulesURLs.nextElement();

                try {
                    // Convert the URL to a URI, then to a Path to read the directory contents
                    Path rulesPath;
                    try {
                        rulesPath = Paths.get(rulesURL.toURI());
                    } catch (FileSystemNotFoundException e) {
                        // Handle the case where the file system is not accessible (e.g., in a JAR)
                        FileSystem fileSystem = FileSystems.newFileSystem(rulesURL.toURI(), Collections.emptyMap());
                        rulesPath = fileSystem.getPath(RULES_PATH);
                    }

                    // Scan the directory for rule files
                    try (Stream<Path> paths = Files.walk(rulesPath)) {
                        paths.filter(Files::isRegularFile)
                                .forEach(rulePath -> {
                                    try {
                                        InputStream ruleInputStream = Files.newInputStream(rulePath);
                                        ruleStreams.add(new RuleStream(rulePath.getFileName().toString(), ruleInputStream));
                                    } catch (IOException e) {
                                        throw new RuntimeException("Failed to load rule: " + rulePath, e);
                                    }
                                });
                    }
                } catch (IOException | URISyntaxException e) {
                    throw new RuntimeException("Failed to scan rules directory on classpath: " + rulesURL, e);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load rules from classpath.", e);
        }

        return ruleStreams;
    }

}
