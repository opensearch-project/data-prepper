Ruby processor to run custom Ruby code from configuration file or a different file. More documentation to come.

# Example
Can specify their Ruby code in the config file, like the following example to translate a message:

```code-string-ruby-pipeline:
  source:
    ...
  processor:
     - ruby:
            code: 'event.put("de_translation", 
            I18n.t(event.get("message"))'
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
                locale: "de"
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



# Configuration options

code — Ruby code that will be run on each Event.

* code and path are mutually exclusive. Exactly one of the two must be specified.
* There is no default.

path — Path to a Ruby file.

* path and code are mutually exclusive. Exactly one of the two must be specified.
* Note that Ruby files specified in path must implement the interface mentioned below.
* There is no default.

init — Ruby code run once at pipeline startup.

* Optional and intended to be used with code. Cannot be used with path. To specify init code with a Ruby file, implement the init method in the Ruby file.
* There is no default.

params — A map of variable names to their values. These are passed into the init method (either from config or in the Ruby file).

* Optional.
* The default is an empty map.
* The type is Map<String, String>.

ignore_exception — Boolean representing whether the processor should ignore exceptions.

* Optional.
* Default: false, meaning the Data Prepper pipeline will be stopped on exception.
* If set to `true`, then events that trigger exceptions when processed will be tagged.  

tags_on_failure — A list of tags to apply to events that trigger exceptions when processed. These tags may be used 
in conditional expressions elsewhere in the configuration. 
* Optional.
* Default: []
* The type is List<String>.
