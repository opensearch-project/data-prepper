package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.plugin.InvalidPluginDefinitionException;
import org.opensearch.dataprepper.model.plugin.PluginInvocationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Named
public class ExtensionPluginCreator {
    private static final Logger LOG = LoggerFactory.getLogger(PluginCreator.class);

    <T> T newPluginInstance(final Class<T> pluginClass,
                            final PluginArgumentsContext pluginArgumentsContext,
                            final String pluginName,
                            final Object... args) {
        Objects.requireNonNull(pluginClass);
        Objects.requireNonNull(pluginArgumentsContext);
        Objects.requireNonNull(pluginName);

        final Constructor<?> constructor = getConstructor(pluginClass, pluginName);

        final Object[] constructorArguments = pluginArgumentsContext.createArguments(constructor.getParameterTypes(), args);

        try {
            return (T) constructor.newInstance(constructorArguments);
        } catch (final IllegalAccessException | InstantiationException ex) {
            LOG.error("Encountered exception while instantiating the plugin {}", pluginClass.getSimpleName(), ex);
            throw new InvalidPluginDefinitionException(
                    "Unable to access or instantiate the plugin '" + pluginClass.getSimpleName() + ".'", ex);
        } catch (final InvocationTargetException ex) {
            LOG.error("Encountered exception while instantiating the plugin {}", pluginClass.getSimpleName(), ex);
            throw new PluginInvocationException("Exception throw from the plugin'" + pluginClass.getSimpleName() + "'." , ex);
        }
    }

    private <T> Constructor<?> getConstructor(final Class<T> pluginClass, final String pluginName) {

        final Constructor<?>[] constructors = pluginClass.getConstructors();

        final Optional<Constructor<?>> annotatedConstructor = getAnnotatedConstructor(pluginClass, constructors);
        if(annotatedConstructor.isPresent())
            return annotatedConstructor.get();

        final Optional<Constructor<?>> defaultConstructor = Arrays.stream(constructors)
                .filter(c -> c.getParameterTypes().length == 0)
                .findFirst();

        if(defaultConstructor.isPresent())
            return defaultConstructor.get();

        final String error =
                String.format("Data Prepper extension plugin %s with name %s does not have a valid plugin constructor. " +
                                "Please ensure the plugin has a constructor that either: " +
                                "1. Is annotated with @DataPrepperPluginConstructor; " +
                                "2. Is the default constructor.",
                        pluginClass.getSimpleName(), pluginName);

        LOG.error("{}", error);
        throw new InvalidPluginDefinitionException(error);
    }

    private Optional<Constructor<?>> getAnnotatedConstructor(final Class<?> pluginClass, final Constructor<?>[] constructors) {
        final List<Constructor<?>> annotatedConstructors = Arrays.stream(constructors)
                .filter(c -> c.isAnnotationPresent(DataPrepperPluginConstructor.class))
                .collect(Collectors.toList());

        if(annotatedConstructors.size() > 1) {
            throw new InvalidPluginDefinitionException("The extension plugin type " + pluginClass +
                    " has more than one constructor annotated with @DataPrepperPluginConstructor. " +
                    "At most one constructor may have this annotation." );
        }

        if(annotatedConstructors.size() == 1) {
            return Optional.of(annotatedConstructors.get(0));
        }
        return Optional.empty();
    }
}
