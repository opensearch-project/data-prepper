# Plugin Development

Data Prepper supports plugins. All sources, buffers, preppers, and processors
are created as Data Prepper plugins.

## Plugin Requirements

Plugins are created as Java classes. They must conform to the following.

* The class must be annotated with [`@DataPrepperPlugin`](../data-prepper-api/src/main/java/com/amazon/dataprepper/model/annotations/DataPrepperPlugin.java)
* The class must be in the `com.amazon.dataprepper.plugins` package
* The class must implement the required interface
* The class must have a valid constructor (see below)

### Plugin Constructors

The preferred way to create a plugin constructor is to choose a single
constructor and annotate it with [`@DataPrepperConstructor`](../data-prepper-api/src/main/java/com/amazon/dataprepper/model/annotations/DataPrepperPluginConstructor.java).
The constructor can only take in class types which are supported by the plugin framework.

The plugin framework can inject the following types into this constructor:

* An instance of the plugin configuration class type as defined by `DataPrepperPlugin::pluginConfigurationType`. The plugin framework will deserialize this type from the Pipeline configuration and supply it in the constructor if requested.
* An instance of `PluginMetrics`.
* An instance of `PluginSetting`.

If your plugin requires no arguments, it can use a default constructor which will be chosen instead.

Additionally, the plugin framework can create a plugin using a single parameter constructor with
a single parameter of type `PluginSetting`. This behavior is deprecated and planned for removal.
