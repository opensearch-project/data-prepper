Ruby processor to run custom Ruby code from configuration file or a different file. More documentation to come.

# Example
Can specify their Ruby code in the config file, like the following example to translate a message:

```code-string-ruby-pipeline:
  source:
    ...
  processor:
     - ruby:
            code: 'event.put("de_translation", 
            I18n.t(event.get("message", String.class)))'
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

def process(events) # code must implement this interface. See [[Ruby interface]] below.
    events.each { |event|
        event.put("{locale}_translation", 
            I18n.t(event.get("message", String.class)))
    }
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

params — A map of variable names to their values. These are passed into the init method.

* Optional and intended to be used with path.
* The default is an empty map.
* The type is Map<String, String>.

send_multiple_events — Boolean representing whether multiple Events should be emitted to the process command.

* Optional and intended to be used with path.
* Default: false
* If true, then the process method will receive all the Events emitted by the input buffer.
* This option is provided to support more flexibility for event processing.

ignore_exception — Boolean representing whether the processor should ignore exceptions.

* Optional.
* Default: false, meaning the Data Prepper pipeline will be stopped on exception.
* Data Prepper supports end-to-end acknowledgement, so setting this to true may cause data loss. 

