# Adding new performance testing simulations

## Simulation coding guideline
- New simulations can be added to the `./performance-test/src/gatling/java/org/opensearch/dataprepper/test/performance` folder.
- Simulations are written in Java.
- Use a class name that describes what type of scenario your simulation is designed to simulate and end with a `Simulation` suffix. 
- Simulations are executed using the Simulations classes static function.
- Test results must be repeatable. No element of the test should depend on a random element.

# Adding command line parameters

Define static final variables within the simulation class itself.
- For integer parameters use `Integer.getInteger(name [, default])`
- For string parameters use `System.getProperty(name [, default])`

```java
// Integer
private static final Integer targetRps = Integer.getInteger("targetRps");
// Integer with default value
private static final Integer maxUsers = Integer.getInteger("maxUsers", 1000);
// String with default value
private static final String targetHost = System.getProperty("targetHost", "http://localhost");
```

Parameters can be set from the command line using the `-D` flag
```bash
./gradlew :performance-test:gatlingRun -DtargetRps=100 -DmaxUsers=10 -DtargetHost=127.0.0.1
```
