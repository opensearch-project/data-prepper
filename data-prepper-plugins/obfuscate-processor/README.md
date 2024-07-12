# Obfuscate Processor

This processor provide Obfuscation on fields to protect sensitive data

## Basic Usage

The basis usage of the processor is as below

```yaml
pipeline:
  source:
    http:
  processor:
    - obfuscate:
        source: "log"
        target: "new_log"
        patterns:
          - "[A-Za-z0-9+_.-]+@([\\w-]+\\.)+[\\w-]{2,4}"
        action:
          mask:
            mask_character: "#"
            mask_character_length: 6
    - obfuscate:
        source: "phone"
  sink:
    - stdout:
```

Take below input

```json
{
  "id": 1,
  "phone": "(555) 555 5555",
  "log": "My name is Bob and my email address is abc@example.com"
}
```

When run, the processor will parse the message into the following output:

```json
{
  "id": 1,
  "phone": "***",
  "log": "My name is Bob and my email address is abc@example.com",
  "newLog": "My name is Bob and my email address is ######"
}
```

And there are some predefined common patterns to simply the use, such as Email address, etc. e.g. Instead of
providing `[A-Za-z0-9+_.-]+@([\\w-]+\\.)+[\\w-]{2,4}`, user can also use `%{EMAIL_ADDRESS}`. Please refer
to [Appendix 1 - Predefined Patterns](#appendix-1---predefined-patterns) for more details.

> Note that the predefined patterns may not cover all cases, and there is no guarantee that they are 100% accurate (e.g.
> 000-00-0000 is also identified as a US SSN number). You can use custom patterns if they can't meet your requirements.

## Configuration

Below are the list of configuration options.

* `source` - (required) - The source field to be obfuscated
* `target` - (optional) - Store the obfuscated value as a new field, leave the source field unchanged. If not provided,
  the source field will be updated with obfuscated value.
* `patterns` - (optional) - A list of Regex patterns. You can define multiple patterns for the same field. Only the
  parts that matched the Regex patterns to be obfuscated. If not provided, the full field will be obfuscated.
* `single_word_only` - (optional) - When set to `true`, a word boundary `\b` is added to the pattern, due to which obfuscation would be applied only to words that are standalone in the input text. By default, it is `false`, meaning obfuscation patterns are applied to all occurrences.
* `action` - (optional) - Obfuscation action, `mask` or `hash` to use one way hashing. Default to `mask` 


### Configuration - Mask Action

There are some additional configuration options for Mask action.

* `mask_character` - (optional) - Default to "*". Valid characters are !, #, $, %, &, *, and @.
* `mask_character_length` - (optional) - Default to 3. The value must be between 1 and 10. There will be n numbers of
  obfuscation characters, e.g. '***'

### Configuration - One Way Hash Action

There are some additional configuration options for One Way Hash action.

* `format` - (optional) - Default to SHA-512. Format of One Way Hash to use. 
* `salt` - (optional) - Default to generate random salt.


---

## FAQ:

**Q1: Can this processor auto-detect the sensitive data to be obfuscated?**

The answer is No. This processor is essentially a mutate of strings based on the pattern provided by users. There is no
NLP feature provided to auto-detect sensitive data in this processor.

**Q2: What are the differences between this one and the SubstituteStringProcessor.**

This processor provide more flexible options such as setting obfuscation character and length etc. to substitute the
string.

**Q3: Can this support one entry with multiple patterns?**

e.g. A field that contains multiple patterns to be obfuscated (like Email, IP, etc.)

One entry will multiple patterns are not supported, you will need to add multiple entries for each pattern
for the same key.

**Q4:  supported patterns?**

e.g. A field that contains multiple patterns to be obfuscated (like Email, IP, etc.)

One entry will multiple patterns are not supported, you will need to add multiple entries for each pattern
for the same key.

## Developer Guide

This plugin is compatible with Java 11 and 17. Refer to the following developer guides for plugin development:

- [Developer Guide](https://github.com/opensearch-project/data-prepper/blob/main/docs/developer_guide.md)
- [Contributing Guidelines](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [Plugin Development](https://github.com/opensearch-project/data-prepper/blob/main/docs/plugin_development.md)
- [Monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)

## Appendix

### Appendix 1 - Predefined Patterns.

Below are the full list of predefined patterns with examples.

| Pattern Name          | Examples                                                                                                                                                                      |
|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| %{EMAIL_ADDRESS}      | abc@test.com<br/> 123@test.com<br/>abc123@test.com<br/>abc_123@test.com<br/>a-b@test.com<br/>a.b@test.com<br/>abc@test-test.com<br/>abc@test.com.cn<br/>abc@test.mail.com.org |
| %{IP_ADDRESS_V4}      | 1.1.1.1<br/>192.168.1.1<br/>255.255.255.0                                                                                                                                     |
| %{BASE_NUMBER}        | 1.1<br/>.1<br/>2000                                                                                                                                                           |
| %{CREDIT_CARD_NUMBER} | 5555555555554444<br/>4111111111111111<br/>1234567890123456<br/>1234 5678 9012 3456<br/> 1234-5678-9012-3456                                                                   |
| %{US_PHONE_NUMBER}    | 1555 555 5555<br/>5555555555<br/>1-555-555-5555<br/>1-(555)-555-5555<br/>1(555) 555 5555<br/>(555) 555 5555<br/>+1-555-555-5555<br/>                                          |
| %{US_SSN_NUMBER}      | 123-11-1234                                                                                                                                                                   |
