package org.opensearch.dataprepper.plugins.processor.ruby;

import org.checkerframework.checker.units.qual.C;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.loggenerator.ApacheLogFaker;
import org.opensearch.dataprepper.plugins.source.loggenerator.LogGeneratorSource;
import org.opensearch.dataprepper.plugins.source.loggenerator.logtypes.CommonApacheLogTypeGenerator;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.array;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

public class RubyProcessorIT {
    private static final int NUMBER_EVENTS_TO_TEST = 100;
    private static final String PLUGIN_NAME = "ruby";
    private static final String TEST_PIPELINE_NAME = "ruby_processor_test";

    static String CODE_STRING = "require 'java'\n" + "puts event.class\n" +
            "event.put('downcase', event.get('message').downcase)";

    private RubyProcessor rubyProcessor;

    private RubyProcessorConfig rubyProcessorConfig;
    private CommonApacheLogTypeGenerator apacheLogGenerator;

    @BeforeEach
    void setup() {
        rubyProcessorConfig = new RubyProcessorConfig();
        try {
            setField(RubyProcessorConfig.class, RubyProcessorIT.this.rubyProcessorConfig, "code", CODE_STRING);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        apacheLogGenerator = new CommonApacheLogTypeGenerator();

        PluginMetrics pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);

        rubyProcessor = new RubyProcessor( pluginMetrics, rubyProcessorConfig);
    }

    @AfterEach
    void tearDown() {
        rubyProcessor.shutdown();
    }

    @Test
    void when_eventMessageIsDowncasedByRubyProcessor_then_eventMessageIsDowncased() {
        final List<Record<Event>> records = IntStream.range(0, NUMBER_EVENTS_TO_TEST)
                .mapToObj(i -> new Record<>(apacheLogGenerator.generateEvent()))
                .collect(Collectors.toList());
        final List<Record<Event>> parsedRecords = (List<Record<Event>>) rubyProcessor.doExecute(records);

        for (int recordNumber = 0; recordNumber < records.size(); recordNumber++) {
            final Event parsedEvent = parsedRecords.get(recordNumber).getData();
            final String originalString = parsedEvent.get("message", String.class);
            assertThat(parsedEvent.get("downcase", String.class), equalTo(originalString.toLowerCase()));
        }
    }

    @Test
    void when_needToCleanScriptAPICallsAndJavaAlreadyRequired_then_works() {
        final List<Record<Event>> records = IntStream.range(0, NUMBER_EVENTS_TO_TEST)
                .mapToObj(i -> new Record<>(apacheLogGenerator.generateEvent()))
                .collect(Collectors.toList());

        CODE_STRING = "require 'java'\n" + "puts event.class\n" +
                "event.put('downcase', event.get('message').downcase)";
        setup();

        final List<Record<Event>> parsedRecords = (List<Record<Event>>) rubyProcessor.doExecute(records);

        for (int recordNumber = 0; recordNumber < records.size(); recordNumber++) {
            final Event parsedEvent = parsedRecords.get(recordNumber).getData();
            final String originalString = parsedEvent.get("message", String.class);
            assertThat(parsedEvent.get("downcase", String.class), equalTo(originalString.toLowerCase()));
        }
    }

    @Test
    void when_needToCleanScriptAPICallsAndJavaNotRequired_then_injectsJavaAndWorks() {
        final List<Record<Event>> records = IntStream.range(0, NUMBER_EVENTS_TO_TEST)
                .mapToObj(i -> new Record<>(apacheLogGenerator.generateEvent()))
                .collect(Collectors.toList());

        CODE_STRING = "event.put('downcase', event.get('message').downcase)";
        setup();

        final List<Record<Event>> parsedRecords = (List<Record<Event>>) rubyProcessor.doExecute(records);

        for (int recordNumber = 0; recordNumber < records.size(); recordNumber++) {
            final Event parsedEvent = parsedRecords.get(recordNumber).getData();
            final String originalString = parsedEvent.get("message", String.class);
            assertThat(parsedEvent.get("downcase", String.class), equalTo(originalString.toLowerCase()));
        }
    }

    @Test
    void when_dotClassMethodCalledAnywhereBesidesEventGetCall_then_remainsUnchanged() {
        final List<Record<Event>> records = IntStream.range(0, NUMBER_EVENTS_TO_TEST)
                .mapToObj(i -> new Record<>(apacheLogGenerator.generateEvent()))
                .collect(Collectors.toList());

        CODE_STRING = "puts event.class\n" +
                "event.put('downcase', event.get('message').downcase)";

        setup();
        final List<Record<Event>> parsedRecords = (List<Record<Event>>) rubyProcessor.doExecute(records);

        assertThat(rubyProcessor.getCodeToExecute().contains("puts event.class"), equalTo(true));
    // todo: will require a refactor to assert
    }

    @Test
    void when_eventAPICallsOverriddenOnString_then_returnedObjectDoesNotHaveArrayFunctionality // or "need not be typecast" or some variation
            () {
        // create event with string field
        final List<Record<Event>> records = IntStream.range(0, NUMBER_EVENTS_TO_TEST)
                .mapToObj(i -> new Record<>(apacheLogGenerator.generateEvent()))
                .collect(Collectors.toList());

        // call get on the event field.
        CODE_STRING = "out_str = event.get('message')\n" +
                        "event.put('message_is_array_and_includes_x', out_str.include?('x')";
        setup();

        assertThrows(Exception.class, () -> rubyProcessor.doExecute(records)); // todo: more descriptive exception

        // make sure that a string-specific operation can be performed
    }

    @Test
    void when_messageFieldIsAnArrayPrimitiveOfObjects_then_rubyArrayOperationsWork() { // todo: Test on actual primitives.
        final List<Record<Event>> records = IntStream.range(0, NUMBER_EVENTS_TO_TEST)
                .mapToObj(i -> new Record<>(apacheLogGenerator.generateEvent()))
                .collect(Collectors.toList());

        for (Record record : records) {
            Event event = (Event) record.getData();
            String[] arrayToPut = {"x", "y", "z"};
            event.put("message", arrayToPut);
        }
        CODE_STRING = "out_arr = event.get('message')\n" +
                "event.put('message_is_array_and_includes_x', out_arr.include?('x'))\n";
        setup();

        final List<Record<Event>> parsedRecords = (List<Record<Event>>) rubyProcessor.doExecute(records);

        for (int recordNumber = 0; recordNumber < records.size(); recordNumber++) {
            final Event parsedEvent = parsedRecords.get(recordNumber).getData();
            assertThat(parsedEvent.get("message_is_array_and_includes_x", Boolean.class), equalTo(true));
        }

    }

    @Test
    void when_messageFieldIsAnArrayPrimitiveOfPrimitives_then_rubyArrayOperationsWork() {
        final List<Record<Event>> records = IntStream.range(0, NUMBER_EVENTS_TO_TEST)
                .mapToObj(i -> new Record<>(apacheLogGenerator.generateEvent()))
                .collect(Collectors.toList());
        final int intOne = 3;
        final int intTwo = -5;
        for (Record record : records) {
            Event event = (Event) record.getData();
            int[] arrayToPut = {intOne, intTwo};
            event.put("message", arrayToPut);
        }

        CODE_STRING = "out_arr = event.getList('message')\n" + // also works with get()
                "event.put('sum', out_arr.get(0) + out_arr.get(1))\n";
        // todo: document that the java, not ruby, list is returned when calling Event.get().
        setup();

        final List<Record<Event>> parsedRecords = (List<Record<Event>>) rubyProcessor.doExecute(records);

        for (int recordNumber = 0; recordNumber < records.size(); recordNumber++) {
            final Event parsedEvent = parsedRecords.get(recordNumber).getData();
            assertThat(parsedEvent.get("sum", Integer.class), equalTo(intOne + intTwo));
        }
    }
        @Test
        void TestArrayListOnObjects( ) {
            final List<Record<Event>> records = IntStream.range(0, NUMBER_EVENTS_TO_TEST)
                    .mapToObj(i -> new Record<>(apacheLogGenerator.generateEvent()))
                    .collect(Collectors.toList());
            for (Record record : records) {
                Event event = (Event) record.getData();
                ArrayList<String> arrayToPut = new ArrayList<>();
                arrayToPut.add("x");
                arrayToPut.add("y");
                arrayToPut.add("z");
                event.put("message", arrayToPut);
            }
            CODE_STRING = "out_arr = event.get('message')\n" +
                    "event.put('message_is_array_and_includes_x', out_arr.include?('x'))\n";
            setup();

            final List<Record<Event>> parsedRecords = (List<Record<Event>>) rubyProcessor.doExecute(records);

            for (int recordNumber = 0; recordNumber < records.size(); recordNumber++) {

            }


        } // todo: test array list

    @Test
    void when_customObjectIsDefinedInRuby_then_itsMethodsCanBeCalledWithinRubyAndAddedToEvent() {
        final List<Record<Event>> records = IntStream.range(0, NUMBER_EVENTS_TO_TEST)
                .mapToObj(i -> new Record<>(apacheLogGenerator.generateEvent()))
                .collect(Collectors.toList());

        CODE_STRING = "event.put('dummy_class_value', $d.get(0) )\n";
        setup();

        String initString = "class DummyClass\n" +
                "  def initialize(value)\n" +
                "    @value = value\n" +
                "  end\n" +
                "\n" +
                "  def get(to_add)\n" +
                "    to_add + @value\n" +
                "  end\n" +
                "end\n" +
                "$d = DummyClass.new(3)\n";
        try {
            setField(RubyProcessorConfig.class, RubyProcessorIT.this.rubyProcessorConfig, "initCode", initString);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        PluginMetrics pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);

        rubyProcessor = new RubyProcessor( pluginMetrics, rubyProcessorConfig);

        final List<Record<Event>> parsedRecords = (List<Record<Event>>) rubyProcessor.doExecute(records);

        for (int recordNumber = 0; recordNumber < parsedRecords.size(); recordNumber++) {
            final Event parsedEvent = parsedRecords.get(recordNumber).getData();
            assertThat(parsedEvent.get("dummy_class_value", Integer.class), equalTo(3));
        }
    }

    @Test
    void when_listCreatedInRuby_then_listRetrievableInJava() {
        final List<Record<Event>> records = IntStream.range(0, NUMBER_EVENTS_TO_TEST)
                .mapToObj(i -> new Record<>(apacheLogGenerator.generateEvent()))
                .collect(Collectors.toList());
        final int intOne = 3;
        final int intTwo = -5;
        for (Record record : records) {
            Event event = (Event) record.getData();
            event.put("intOne", intOne);
            event.put("intTwo", intTwo);
        }

        CODE_STRING = "event.put('out_list', [event.get('intOne'), event.get('intTwo')])\n";

        setup();

        final List<Record<Event>> parsedRecords = (List<Record<Event>>) rubyProcessor.doExecute(records);


        for (int recordNumber = 0; recordNumber < parsedRecords.size(); recordNumber++) {
            final Event parsedEvent = parsedRecords.get(recordNumber).getData();
            assertThat(parsedEvent.getList("out_list", Integer.class), equalTo(Arrays.asList(intOne, intTwo)));
        }
    }

    @Test
    void when_timeObjectUsedInRuby() {
        // todo: dealing with Time in Ruby might be dicey.
    }

    @Test
    void when_customClassDefinedInInitCode_then_rubyClassUsableInEvents() {

    }

    @Test
    void when_requireUsedInInit_then_packageUsableInEvents() {
        CODE_STRING = "event.put('date', Date.new(2023, 6, 12))\n";
        setup();
        final String initString = "require 'date'";
        try {
            setField(RubyProcessorConfig.class, RubyProcessorIT.this.rubyProcessorConfig, "initCode", initString);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        PluginMetrics pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);

        rubyProcessor = new RubyProcessor( pluginMetrics, rubyProcessorConfig);


        final List<Record<Event>> records = IntStream.range(0, NUMBER_EVENTS_TO_TEST)
                .mapToObj(i -> new Record<>(apacheLogGenerator.generateEvent()))
                .collect(Collectors.toList());

        final List<Record<Event>> parsedRecords = (List<Record<Event>>) rubyProcessor.doExecute(records);

        for (int recordNumber = 0; recordNumber < parsedRecords.size(); recordNumber++) {
            final Event parsedEvent = parsedRecords.get(recordNumber).getData();
            assertThat(parsedEvent.get("date", Calendar.class).get(Calendar.YEAR),
                    equalTo(2023));
            assertThat(parsedEvent.get("date", Calendar.class).get(Calendar.DAY_OF_MONTH),
                    equalTo(12));

            // todo: make the dates generic.
        }

    }

    @Test
    void when_codeFromFileAndContainsProcessMethodWithCorrectSignature_then_processorWorks()
        throws IOException
    {
        String code = "def process(event)\n" +
                "event.put('processed', true)\n" +
                "end\n";

        final List<Record<Event>> parsedRecords = runGenericStartupCode(code);
        for (int recordNumber = 0; recordNumber < parsedRecords.size(); recordNumber++) {
            final Event parsedEvent = parsedRecords.get(recordNumber).getData();
            assertThat(parsedEvent.get("processed", Boolean.class), equalTo(true));
        }
    }

    @Test
    void when_codeFromFileWithInitAndParams_then_processorWorks()
            throws IOException {


        String code = "def init(params)\n" +
                "$global_var = params.get('message_to_write')\n" +
                "end\n" +
                "def process(event)\n" +
                "event.put('processed', true)\n" +
                "event.put('message_written', $global_var)\n" +
                "end\n";

        Map<String, String> params = Map.of("message_to_write", "hello world");

        String testDataFilePath = "LocalInputFileTest";

        File testDataFile = File.createTempFile(testDataFilePath, "rb");

        writeRubyCodeToFile(testDataFile, code);

        setup();

        try {
            setField(RubyProcessorConfig.class, RubyProcessorIT.this.rubyProcessorConfig, "path",
                    testDataFile.getAbsolutePath());
            // testDataFilePath + ".rb");
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        try {
            setField(RubyProcessorConfig.class, RubyProcessorIT.this.rubyProcessorConfig, "params",
                    params
                    );

        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        PluginMetrics pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);

        rubyProcessor = new RubyProcessor( pluginMetrics, rubyProcessorConfig);

        final List<Record<Event>> records = IntStream.range(0, NUMBER_EVENTS_TO_TEST)
                .mapToObj(i -> new Record<>(apacheLogGenerator.generateEvent()))
                .collect(Collectors.toList());

        final List<Record<Event>> parsedRecords = (List<Record<Event>>) rubyProcessor.doExecute(records);



        for (int recordNumber = 0; recordNumber < parsedRecords.size(); recordNumber++) {
            final Event parsedEvent = parsedRecords.get(recordNumber).getData();
            assertThat(parsedEvent.get("processed", Boolean.class), equalTo(true));
            assertThat(parsedEvent.get("message_written", String.class), equalTo("hello world"));
        }

    }


    // todo: test params when assigned to global var can be used within process
    @Test
    void when_codeFromFileAndDoesNotContainProcessMethod_then_exceptionThrown()
            throws IOException
    {
        String code = "def init\n" +
                        "end\n";
        assertThrows(RuntimeException.class, () -> runGenericStartupCode(code));
    }

    @Test
    void when_codeFromFileAndContainsTooManyInitParameters_then_exceptionThrown()
            throws IOException
    {
        String code = "def init(params1, params2)\n" +
                "end\n"
                + "def process(event)\n" +
                "end\n";
        assertThrows(RuntimeException.class, () -> runGenericStartupCode(code));
    }

    @Test
    void when_codeFromFileAndContainsProcessMethodNoParameters_then_exceptionThrown()
            throws IOException {
        String code = "def process\n" +
                "end\n";

        assertThrows(RuntimeException.class, () -> runGenericStartupCode(code));
    }

    @Test
    void when_codeFromFileAndContainsProcessMethodWithTooManyParameters_then_exceptionThrown()
            throws IOException {
        String code = "def process(event, event2)\n" +
                "end\n";

        assertThrows(RuntimeException.class, () -> runGenericStartupCode(code));
    }

    @Test
    void when_withConfigCodeAndParamsCalledInProcess_then_exceptionThrown() {
        rubyProcessorConfig = new RubyProcessorConfig();

        String code =
                "puts params.get('message_to_write')\n" +
                "event.put('processed', true)\n" +
                "event.put('message_written', $global_var)\n";

        try {
            setField(RubyProcessorConfig.class, RubyProcessorIT.this.rubyProcessorConfig, "code",
            code
                    );

        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        Map<String, String> params = Map.of("message_to_write", "hello world");

        try {
            setField(RubyProcessorConfig.class, RubyProcessorIT.this.rubyProcessorConfig, "params",
                    params
            );

            } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        apacheLogGenerator = new CommonApacheLogTypeGenerator();

        PluginMetrics pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);

        rubyProcessor = new RubyProcessor( pluginMetrics, rubyProcessorConfig);

        final List<Record<Event>> records = IntStream.range(0, NUMBER_EVENTS_TO_TEST)
                .mapToObj(i -> new Record<>(apacheLogGenerator.generateEvent()))
                .collect(Collectors.toList());

        assertThrows(Exception.class, () -> rubyProcessor.doExecute(records));

    }


    // todo: assert test params not accessible outside method
    @Test
    void when_withFileAndParamsCalledInProcess_then_exceptionThrown()
            throws IOException {


        String code = "def init(params)\n" +
                "$global_var = params.get('message_to_write')\n" +
                "end\n" +
                "def process(event)\n" +
                "puts params.get('message_to_write')\n" +
                "event.put('processed', true)\n" +
                "event.put('message_written', $global_var)\n" +
                "end\n";

        Map<String, String> params = Map.of("message_to_write", "hello world");

        String testDataFilePath = "LocalInputFileTest";

        File testDataFile = File.createTempFile(testDataFilePath, "rb");

        writeRubyCodeToFile(testDataFile, code);

        setup();

        try {
            setField(RubyProcessorConfig.class, RubyProcessorIT.this.rubyProcessorConfig, "path",
                    testDataFile.getAbsolutePath());
            // testDataFilePath + ".rb");
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        try {
            setField(RubyProcessorConfig.class, RubyProcessorIT.this.rubyProcessorConfig, "params",
                    params
            );

        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        PluginMetrics pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);

        rubyProcessor = new RubyProcessor( pluginMetrics, rubyProcessorConfig);

        final List<Record<Event>> records = IntStream.range(0, NUMBER_EVENTS_TO_TEST)
                .mapToObj(i -> new Record<>(apacheLogGenerator.generateEvent()))
                .collect(Collectors.toList());

        assertThrows(Exception.class, () -> rubyProcessor.doExecute(records));
    }

    @Test
    void when_paramsThenAccessibleWithRubyHashIndexing() {
        // todo: or should behavior just be java-like?
    }

    @Test
    void when_initCodeDefinedInConfigWithParams_then_paramsAccessible() {

        String initCode =
                "$global_var = params.get('message_to_write')\n";


                String code = "event.put('processed', true)\n" +
                "event.put('message_written', $global_var)\n";


        Map<String, String> params = Map.of("message_to_write", "hello world");

        try {
            setField(RubyProcessorConfig.class, RubyProcessorIT.this.rubyProcessorConfig, "initCode",
                    initCode
            );

        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        try {
            setField(RubyProcessorConfig.class, RubyProcessorIT.this.rubyProcessorConfig, "code",
                    code
            );

        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        try {
            setField(RubyProcessorConfig.class, RubyProcessorIT.this.rubyProcessorConfig, "params",
                    params
            );

        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        apacheLogGenerator = new CommonApacheLogTypeGenerator();

        PluginMetrics pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);

        rubyProcessor = new RubyProcessor( pluginMetrics, rubyProcessorConfig);

        final List<Record<Event>> records = IntStream.range(0, NUMBER_EVENTS_TO_TEST)
                .mapToObj(i -> new Record<>(apacheLogGenerator.generateEvent()))
                .collect(Collectors.toList());
        final List<Record<Event>> parsedRecords = (List<Record<Event>>) rubyProcessor.doExecute(records);



        for (int recordNumber = 0; recordNumber < parsedRecords.size(); recordNumber++) {
            final Event parsedEvent = parsedRecords.get(recordNumber).getData();
            assertThat(parsedEvent.get("processed", Boolean.class), equalTo(true));
            assertThat(parsedEvent.get("message_written", String.class), equalTo("hello world"));
        }

    }

    @Test
    void utility_testThatInitRegexMatches() {
        String[] listOfTestCode = {
                "def init(params)\n\nend",
                "def init(parameter_name)",
                "def init(parameter_name, parameter_name2)",
                "def init",
                "def init()",
                "def init(1h)",
                "def init(_local)",
        };
        ArrayList<Boolean> expectedRegexMatches = new ArrayList<>(List.of(true, true, false, false,  false, false, true));
        String pattern = "def init\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*)\\s*\\)";

        assertMatches(listOfTestCode, expectedRegexMatches, pattern);
    }

    @Test
    void utility_testThatProcessRegexMatches() {
        String[] listOfTestCode = {
                "def process(parameter_name)",
                "def process(parameter_name, parameter_name2)",
                "def process",
                "def process()",
                "def process(1h)",
                "def process(event)",
        };
        ArrayList<Boolean> expectedRegexMatches = new ArrayList<>(List.of(true, false, false,  false, false, true));
        String pattern = "def process\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*)\\s*\\)";

        assertMatches(listOfTestCode, expectedRegexMatches, pattern);
    }

    private void assertMatches(String[] toMatch, ArrayList<Boolean> expectedRegexMatches, String pattern) {
        Pattern compiledPattern = Pattern.compile(pattern);

        ArrayList<Boolean> regexMatches = new ArrayList<>();
        for (String rubyCode : toMatch ) {
            Matcher matcher = compiledPattern.matcher(rubyCode);
            regexMatches.add(matcher.find());
        }
        assertThat(regexMatches, equalTo(expectedRegexMatches));
    }


    private List<Record<Event>> runGenericStartupCode(String code) throws IOException {

        String testDataFilePath = "LocalInputFileTest";

        File testDataFile = File.createTempFile(testDataFilePath, "rb");

        writeRubyCodeToFile(testDataFile, code);

        setup();

        try {
            setField(RubyProcessorConfig.class, RubyProcessorIT.this.rubyProcessorConfig, "path",
                    testDataFile.getAbsolutePath());
            // testDataFilePath + ".rb");
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        PluginMetrics pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);

        rubyProcessor = new RubyProcessor( pluginMetrics, rubyProcessorConfig);

        final List<Record<Event>> records = IntStream.range(0, NUMBER_EVENTS_TO_TEST)
                .mapToObj(i -> new Record<>(apacheLogGenerator.generateEvent()))
                .collect(Collectors.toList());

        final List<Record<Event>> parsedRecords = (List<Record<Event>>) rubyProcessor.doExecute(records);
        return parsedRecords;
    }

    private void writeRubyCodeToFile(File file, String code) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, StandardCharsets.UTF_8))) {
            writer.write(code);
        } catch (IOException e) {
            System.err.println("An error occurred while writing to the file: " + e.getMessage());
        }
    }



}
