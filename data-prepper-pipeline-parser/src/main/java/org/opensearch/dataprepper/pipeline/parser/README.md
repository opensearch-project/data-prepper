## Configuration Transformation
Supports transformation of configuration from user provided configuration to
a transformed configuration based on template and rules.

## Usage

User give configuration passes through rules, if the rules are valid,
the template for the transformations are dynamically chosen and applied.

**User config**

**Template**

**Rule**

**Expected Transformed Config**


### Assumptions
1. Deep scan or recursive expressions like`$..` is NOT supported. Always use a more specific expression.
In the event specific variables in a path are not known, use wildcards.
2. User could provide multiple pipelines in their user config but 
there can be only one pipeline that can support transformation.
3. There cannot be multiple transformations in a single pipeline.
4. `{{ .. }}` is the placeholder in the template.
`{{ pipeline-name }}` is handled differently as compared to other placeholders
as other placeholders are jsonPaths.