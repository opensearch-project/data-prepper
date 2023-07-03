Ruby processor that executes custom Ruby code to manipulate Events.
## Example
You can specify Ruby code in the config file, like the following example that translates a message:

```yaml
code-string-ruby-pipeline:
  source:
    ...
  processor:
     - ruby:
        init: "require 'i18n'\n I18n.locale = 'ie'"
        code: "event.put('message_translation', 
            I18n.t(event.get('message'))"
  sink:
    ...
```
Or use a Ruby file — here is the same example, using a file:
```yaml
ruby-file-pipeline:
  source:
      ...
  processor:
     - ruby:
            file: "/path/to/file/translate_events.rb"
            params:
                locale: "ie"
  sink:
    ...
```

```Ruby
# translate_events.rb
def init(params) # code must implement this interface. See [[Ruby interface]] below.
    require 'I18n'
    locale = params["locale"]
    I18n.locale = locale
end

def process(event) # code must implement this interface. See [[Ruby interface]] below.
        event.put("message_translation", 
            I18n.t(event.get("message"))
        )
end
```
## Configuration options

`code` — Ruby code that will be run on each Event.

* `code` and `path` are mutually exclusive. Exactly one of the two must be specified.
* There is no default.

`path` — Absolute path to a Ruby file.

* `path` and `code` are mutually exclusive. Exactly one of the two must be specified.
* Note that Ruby files specified in `path` must implement the interface mentioned below.
* The path semantics are equivalent to the `File` source and sink
  * For example, if running the [Data Prepper Docker image](https://github.com/opensearch-project/data-prepper/blob/main/docs/getting_started.md#running), append the volume `/usr/share/{path_you_put_in_config}`.
    * If your Ruby code file, `ruby_code.rb`, lives at the Data Prepper root and you are running the image from the command line, append `-v ${PWD}/ruby_code.rb:/usr/share/data-prepper/ruby_code.rb` to the `docker run` command.  
* There is no default.

`init` — Ruby code run once at pipeline startup.

* Optional and intended to be used with `code` option. Cannot be used with `path` option. To specify init code with a Ruby file, implement the init method in the Ruby file (`def init(params)`).
* There is no default.

`params` — A map of variable names to their values. These are passed into the init method (either from config or in the Ruby file).

* Optional.
* Default: `{}`.
* The type is `Map<String, String>`.

`tags_on_failure` — A list of tags to apply to events that trigger exceptions when processed. These tags may be used 
in conditional expressions elsewhere in the configuration. 
* Optional.
* Default: `[]`.
* The type is `List<String>`.
## Ruby Interface

The Event interface is accessible from within Ruby processor code. We have added two new methods to the Event API, `get({field})` and `getList({field})` to accomodate the JRuby processor. Please use these methods instead of `get({field}, {value return class})` because JRuby classes are not a subset of Java classes, leading to type incompatibility. For example, if you were to call `event.get('message', String.class)` from within Ruby, the Event API would be called with `String.class` as the JRuby String class, which would cause an exception.  

If executing Ruby code from a file, you must **implement the following interface**:

```Ruby
def init(params)
    # optional. params is a Map<String, String> from the config.
    # code that is run once at pipeline startup.
end

def process(event)
    # required.
    # event processing logic. The changed state of events is available in Java.
end
```

## A Word of Warning: JRuby and Java Types

Because Events and Data Prepper are written in Java, all the objects within Events are Java objects.

If you are calling `event.get()` on a field that you expect to return an `ArrayList`, do note that you must call Java `ArrayList` methods on the returned object, **even within Ruby**. Instead of writing `arrayObject[1]` from within your Ruby processor code, call `arrayObject.get(1)`. 

Since Ruby is dynamically typed, you do not need to (and should not) typecast objects. Instead, ensure that the method you are calling is implemented by the callee object.


## Ruby Version

The Ruby processor executes Ruby code on the JVM using JRuby 9.4.2.  

## Ruby Gems

Ruby has an ecosystem of extensions called Gems. The Ruby processor is bundled with the following utility gems:
* jmespath
* diff-lcs
* i18n
* tzinfo
* json
* mime-types
* concurrent-ruby
* xml-simple
* nokogiri (java)

These gems are compatible with JRuby 9.4.2 and are bundled in a jar file. If you wish to add additional gems, please open an issue or build Data Prepper locally with your desired gems.

You can bundle in additional gems through the following process:
1. Modify the `Gemfile`. Please make sure that the Ruby and JRuby versions in the Gemfile match with the JRuby version in `build.gradle`.
2. Run the following Gradle task: `gradle runBundleInstall`
   1. This task uses Bundler, a Ruby package manager, to install the gems and package them into a jar.
3. Please note that not every gem is compatible with JRuby. Gems that rely on C extensions are unlikely to be compatible.
