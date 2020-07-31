/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tresys.nifi;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.daffodil.japi.Daffodil;
import org.apache.daffodil.japi.Compiler;
import org.apache.daffodil.japi.DataProcessor;
import org.apache.daffodil.japi.Diagnostic;
import org.apache.daffodil.japi.InvalidUsageException;
import org.apache.daffodil.japi.ProcessorFactory;
import org.apache.daffodil.japi.ValidationMode;
import org.apache.daffodil.japi.WithDiagnostics;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import scala.Predef;
import scala.collection.JavaConverters;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractDaffodilProcessor extends AbstractProcessor {

    protected abstract boolean isUnparse();

    public static final PropertyDescriptor DFDL_SCHEMA_FILE = new PropertyDescriptor.Builder()
            .name("dfdl-schema-file")
            .displayName("DFDL Schema File")
            .description("Full path to the DFDL schema file that is to be used for parsing/unparsing.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for reading incoming data")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("cache-size")
            .displayName("Cache Size")
            .description("Maximum number of compiled DFDL schemas to cache. Zero disables the cache.")
            .required(true)
            .defaultValue("50")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor CACHE_TTL_AFTER_LAST_ACCESS = new PropertyDescriptor.Builder()
            .name("cache-ttl-after-last-access")
            .displayName("Cache TTL After Last Access")
            .description("The cache TTL (time-to-live) or how long to keep compiled DFDL schemas in the cache after last access.")
            .required(true)
            .defaultValue("30 mins")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully transformed will be routed to this relationship")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile cannot be transformed from the configured input format to the configured output format, "
                    + "the unchanged FlowFile will be routed to this relationship")
            .build();

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    static final String OFF_VALUE = "off";
    static final String LIMITED_VALUE = "limited";
    static final String FULL_VALUE = "full";

    static final AllowableValue VALIDATION_MODE_OFF
        = new AllowableValue(OFF_VALUE, OFF_VALUE, "Disable infoset validation");
    static final AllowableValue VALIDATION_MODE_LIMITED
        = new AllowableValue(LIMITED_VALUE, LIMITED_VALUE, "Facet/restriction validation using Daffodil");
    static final AllowableValue VALIDATION_MODE_FULL
        = new AllowableValue(FULL_VALUE, FULL_VALUE, "Full schema validation using Xerces");

    public static final PropertyDescriptor VALIDATION_MODE = new PropertyDescriptor.Builder()
            .name("validation-mode")
            .displayName("Validation Mode")
            .description("The type of validation to be performed on the infoset.")
            .required(true)
            .defaultValue(OFF_VALUE)
            .allowableValues(VALIDATION_MODE_OFF, VALIDATION_MODE_LIMITED, VALIDATION_MODE_FULL)
            .build();

    /**
     * Regular expression patterns for parsing the External Variables and Tunables Properties
     *
     * The Tunables Property expects a list of key-value pairs in the form of key=value separated by whitespace,
     * and the External Variables Property expects a list of key-value pairs in the form of {namespace}key=value separated by whitespace.
     */
    private static final Pattern TUNABLES_PATTERN = Pattern.compile("(\\S+)=(\\S+)");
    private static final Pattern EXT_VAR_PATTERN = Pattern.compile("((?:\\{.*})*\\S+)=(\\S+)");

    /**
     * Converts either a list of external variables or tunable variables to a Map<String, String> based on the
     * regular expression provided
     * @param input the raw list of variables or tunables to be parsed
     * @param pairValidator the regular expression to be used to parse the input
     * @return a Map<String, String> if the input is successfully parsed with the key-value pairs being the
     *         values of various external variable or tunable properties set.
     * @throws IOException if the given input does not match the expected format defined by the regular expression.
     */
    private static HashMap<String, String> variableListToMap(String input, Pattern pairValidator) throws IOException {
        if (input == null || input.replaceAll("\\s+", "").isEmpty()) {
            return new HashMap<>();
        } else {
            HashMap<String, String> variableMap = new HashMap<>();
            String[] pairs = input.split("\\s+");
            for (String pair: pairs) {
                Matcher pairMatcher = pairValidator.matcher(pair);
                if (!pairMatcher.find()) {
                    throw new IOException(String.format("Invalid format for KEY=VALUE pair '%s'!", input));
                } else {
                    String key = pairMatcher.group(1);
                    String value = pairMatcher.group(2);
                    variableMap.put(key, value);
                }
            }
            return variableMap;
        }
    }

    /**
     * Custom Validator to ensure the External Variables and Tunables Properties can be parsed.
     * Fails if variableListToMap throws an IOException when the input is passed, succeeds otherwise.
     */
    private static class VariableListValidator implements Validator {
        private final Pattern variableListPattern;

        public VariableListValidator(Pattern variableListPattern) {
            this.variableListPattern = variableListPattern;
        }

        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            String errorMessage = "";
            try {
                variableListToMap(input, variableListPattern);
            } catch (IOException ioe) {
                errorMessage = ioe.getMessage();
            }
            String explanation = errorMessage.isEmpty() ? "Valid input" : errorMessage;
            return new ValidationResult.Builder()
                    .explanation(explanation)
                    .input(input)
                    .subject(subject)
                    .valid(errorMessage.isEmpty())
                    .build();
        }
    }

    private static final Validator TUNABLES_VALIDATOR = new VariableListValidator(TUNABLES_PATTERN);
    private static final Validator EXT_VAR_VALIDATOR = new VariableListValidator(EXT_VAR_PATTERN);

    public static final PropertyDescriptor TUNABLE_LIST = new PropertyDescriptor.Builder()
            .name("tunables")
            .displayName("Tunables")
            .description("A list of tunable key-value pairs to configure the Daffodil Compiler."
                    + " Each pair should be in the form KEY=VALUE and separated by whitespace.")
            .required(false)
            .addValidator(TUNABLES_VALIDATOR)
            .build();

    public static final PropertyDescriptor EXTERNAL_VARIABLES_LIST = new PropertyDescriptor.Builder()
            .name("external_variables")
            .displayName("External Variables")
            .description("A list of external variable key-value pairs to be applied to Daffodil parsing."
                + " Each pair should be in the form {NAMESPACE}KEY=VALUE and separated by whitespace.  {NAMESPACE} is optional.")
            .required(false)
            .addValidator(EXT_VAR_VALIDATOR)
            .build();

    /**
     * Currently only external variables are supported as configuration file items.
     */
    public static final PropertyDescriptor CONFIG_FILE = new PropertyDescriptor.Builder()
            .name("config_file")
            .displayName("DFDL Config File Path")
            .description("Path to an XML-based DFDL Configuration file that contains a list of external variables and tunables.")
            .required(false)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    /**
     * If this Property is set to true, then multiple Records will be produced when there is leftover data, with each one beginning where
     * the last one left off.  Normally leftover data just errors out.  We will still route to failure if *any* of these Records
     * are not successfully produced.  Making this option true does not cause any issues for unparsing, as the unparse Record component is
     * a RecordSetWriterFactory, which is able to handle the data containing a set of Records rather than just one Record.
     */
    public static final PropertyDescriptor STREAM_MODE = new PropertyDescriptor.Builder()
            .name("stream_mode")
            .displayName("Stream Mode is activated.")
            .description("Rather than throwing an error when left over data exists after a parse, repeat the parse with the remaining data. "
                + "Parsing repeats until end of data is reached, an error occurs, or no data is consumed"
            )
            .required(true)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();


    private static final List<PropertyDescriptor> daffodilProperties;

    static {
        final List<PropertyDescriptor> propertyList = new ArrayList<>();
        propertyList.add(DFDL_SCHEMA_FILE);
        propertyList.add(VALIDATION_MODE);
        propertyList.add(CACHE_SIZE);
        propertyList.add(CACHE_TTL_AFTER_LAST_ACCESS);
        propertyList.add(TUNABLE_LIST);
        propertyList.add(EXTERNAL_VARIABLES_LIST);
        propertyList.add(CONFIG_FILE);
        propertyList.add(STREAM_MODE);
        daffodilProperties = Collections.unmodifiableList(propertyList);
    }

    private LoadingCache<String, ProcessorFactory> cache;

    /**
     * Note that the Reader/Writer component always winds up at index 1; we choose which one to add based
     * on if this is the Parse or Unparse Processor.
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.addAll(daffodilProperties);
        if (isUnparse()) {
            properties.add(1, RECORD_READER);
        } else {
            properties.add(1, RECORD_WRITER);
        }
        return properties;
    }

    /**
     * Creates a new ProcessorFactory object from the provided schema file path.
     * The Compiler used to create this ProcessorFactory can be configured with the tunableList passed in.
     * @param dfdlSchema the path to the DFDL schema file used to create this ProcessorFactory
     * @param tunableList a list of key-value pairs for tunable values to be parsed into a Map for Compiler configuration
     * @return a ProcessorFactory object as described above
     * @throws IOException if the tunable list cannot be parsed or the Schema compilation fails
     */
    private ProcessorFactory newProcessorFactory(String dfdlSchema, String tunableList) throws IOException {
        File f = new File(dfdlSchema);
        Compiler daffodilCompiler = Daffodil.compiler();

        HashMap<String, String> tunableMap = variableListToMap(tunableList, TUNABLES_PATTERN);
        if (!tunableMap.keySet().isEmpty()) {
            daffodilCompiler = daffodilCompiler.withTunables(tunableMap);
        }

        ProcessorFactory pf = daffodilCompiler.compileFile(f);
        if (pf.isError()) {
            getLogger().error("Failed to compile DFDL schema: " + dfdlSchema);
            logDiagnostics(pf);
            throw new IOException("Failed to compile DFDL schema: " + dfdlSchema);
        }
        return pf;
    }

    /**
     * @param dfdlSchema the path to the DFDL schema file used to create this ProcessorFactory
     * @param tunableList a list of key-value pairs for tunable values to be parsed into a Map for Compiler configuration
     * @return an existing ProcessorFactory object if it was already generated and cached, otherwise a new ProcessorFactory object
     * @throws IOException if creating a new ProcessorFactory object fails or a PF expected to be cached is not successfully obtained.
     */
    protected ProcessorFactory getProcessorFactory(String dfdlSchema, String tunableList) throws IOException {
        if (cache != null) {
            try {
                return cache.get(dfdlSchema);
            } catch (ExecutionException e) {
                throw new IOException("Could not obtain Processor from cache: " + e);
            }
        } else {
            return newProcessorFactory(dfdlSchema, tunableList);
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final ComponentLog logger = getLogger();
        final Integer cacheSize = context.getProperty(CACHE_SIZE).asInteger();
        final Long cacheTTL = context.getProperty(CACHE_TTL_AFTER_LAST_ACCESS).asTimePeriod(TimeUnit.SECONDS);
        final String tunableList = context.getProperty(TUNABLE_LIST).getValue();

        // update the current cache to contain a new ProcessorFactory object whenever onScheduled is called.
        // The cache's key values are based on the path to the schema file.
        if (cacheSize > 0) {
            CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder().maximumSize(cacheSize);
            if (cacheTTL > 0) {
                cacheBuilder.expireAfterAccess(cacheTTL, TimeUnit.SECONDS);
            }
            cache = cacheBuilder.build(
                new CacheLoader<String, ProcessorFactory>() {
                    public ProcessorFactory load(String dfdlSchema) throws IOException {
                        return newProcessorFactory(dfdlSchema, tunableList);
                    }
                }
            );
        } else {
            cache = null;
            logger.warn("Daffodil data processor cache disabled because cache size is set to 0.");
        }
    }

    protected void logDiagnostics(WithDiagnostics diagnostics) {
        final ComponentLog logger = getLogger();
        final List<Diagnostic> diags = diagnostics.getDiagnostics();
        for (Diagnostic diag : diags) {
            String message = diag.toString();
            if (diag.isError()) {
                logger.error(message);
            } else {
                logger.warn(message);
            }
        }
    }

    /**
     * Converts a Java Map to a Scala Map.  This method is needed to call the withExternalVariables and withTunables
     * methods, which are both in the Daffodil Java API yet take Scala objects as parameters.
     * @param javaMap a Java Map
     * @return the Java Map converted to a Scala Map
     */
    private static scala.collection.immutable.Map<String, String> hashMapToScalaMap(Map<String, String> javaMap) {
        return JavaConverters.mapAsScalaMapConverter(javaMap).asScala().toMap(Predef.$conforms());
    }

    /**
     * Given a ProcessorFactory object that has already been configured with Tunables, returns a DataProcessor
     * object configured with a validation mode, config file, and external variables
     * @param context the ProcessContext from which to obtain these configuration options
     * @param pf the given ProcessorFactory from which a DataProcessor will be built
     * @return a DataProcessor object as described above
     * @throws IOException if there is an issue in setting one of the config options or if onPath results in
     *                     an errored DataProcessor
     */
    protected DataProcessor getConfiguredDataProcessor(ProcessContext context, ProcessorFactory pf) throws IOException {
        ComponentLog logger = getLogger();
        final ValidationMode validationMode;
        final String extVariablesList = context.getProperty(EXTERNAL_VARIABLES_LIST).getValue();
        final String configFilePath = context.getProperty(CONFIG_FILE).getValue();

        switch (context.getProperty(VALIDATION_MODE).getValue()) {
            case OFF_VALUE:
                validationMode = ValidationMode.Off;
                break;
            case LIMITED_VALUE:
                validationMode = ValidationMode.Limited;
                break;
            case FULL_VALUE:
                validationMode = ValidationMode.Full;
                break;
            default: throw new AssertionError("validation mode was not one of 'off', 'limited', or 'full'");
        }

        try {
            DataProcessor dp = pf.onPath("/");
            try {
                dp = dp.withValidationMode(validationMode);
                HashMap<String, String> extVariableMap = variableListToMap(extVariablesList, EXT_VAR_PATTERN);
                if (!extVariableMap.isEmpty()) {
                    dp = dp.withExternalVariables(hashMapToScalaMap(extVariableMap));
                }
                if (configFilePath != null && !configFilePath.replaceAll("\\s", "").isEmpty()) {
                    dp = dp.withExternalVariables(new File(configFilePath));
                }
                if (dp.isError()) {
                    throw new ProcessException("DataProcessor error: " + dp.getDiagnostics().toString());
                }
                return dp;
            } catch (InvalidUsageException e) {
                throw new IOException("Invalid usage", e);
            }
        } catch (ProcessException e) {
            logger.error("Failed to process due to {}", new Object[]{e});
            throw new IOException("Process Exception", e);
        }
    }

    /**
     * Gets a NiFi RecordSchema from a passed in ProcessorFactory object using the Daffodil DSOM API
     * and a custom RecordWalker implementation of that API
     * @param pf the given ProcessorFactory object
     * @return a RecordSchema corresponding to pf
     * @throws IOException if the passed in ProcessorFactory is in an errored state
     */
    protected static RecordSchema getSchema(ProcessorFactory pf) throws IOException {

        if (pf.isError()) {
            throw new IOException(pf.getDiagnostics().toString());
        }
        RecordWalker walker = new RecordWalker();
        walker.walkFromRoot(pf.experimental().rootView());
        return walker.getResult();
    }

    /**
     * This onTrigger method is nearly line-for-line copied from AbstractRecordProcessor
     *
     * The only reason why this class does not extend AbstractRecordProcessor is because there was not a good way
     * to systematically set the RECORD_READER and RECORD_WRITER Properties with all the given configurations
     *
     * Plus, at least for the moment, we have to do a bit of filtering for the output of any XML Infoset
     * due to the bug that currently exists on that processor.
     *
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String dfdlSchema = context.getProperty(DFDL_SCHEMA_FILE).evaluateAttributeExpressions(flowFile).getValue();
        final boolean streamMode = context.getProperty(STREAM_MODE).asBoolean();
        final String tunableList = context.getProperty(TUNABLE_LIST).getValue();

        final Map<String, String> attributes = new HashMap<>();
        final AtomicInteger recordCount = new AtomicInteger();

        final FlowFile original = flowFile;
        final Map<String, String> originalAttributes = flowFile.getAttributes();
        try {
            ProcessorFactory pf = getProcessorFactory(dfdlSchema, tunableList);
            RecordSchema schema = getSchema(pf);
            DataProcessor configuredProcessor;
            try {
                configuredProcessor = getConfiguredDataProcessor(context, pf);
            } catch (IOException ioe) {
                throw new ProcessException("Could not obtain Data Processor: ", ioe);
            }
            flowFile = session.write(flowFile, (inputStream, outputStream) -> {

                // TODO delete the actualOut variable once the bug mentioned in the TODO below is fixed
                OutputStream actualOut = isUnparse() ? outputStream : new ByteArrayOutputStream();

                final RecordReaderFactory readerFactory;
                if (isUnparse()) {
                    readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
                } else {
                    readerFactory = new DaffodilParse.DFDLReaderFactory(schema, inputStream, configuredProcessor, streamMode);
                }

                final RecordSetWriterFactory writerFactory;
                if (isUnparse()) {
                    writerFactory = new DaffodilUnparse.DFDLWriterFactory(configuredProcessor, schema, streamMode);
                } else {
                    writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
                }

                try (final RecordReader reader = readerFactory.createRecordReader(originalAttributes, inputStream, original.getSize(), getLogger())) {
                    // Get the first record and process it before we create the Record Writer. We do this so that if the Processor
                    // updates the Record's schema, we can provide an updated schema to the Record Writer. If there are no records,
                    // then we can simply create the Writer with the Reader's schema and begin & end the Record Set.
                    Record firstRecord = reader.nextRecord();
                    if (firstRecord == null) {
                        final RecordSchema writeSchema = writerFactory.getSchema(originalAttributes, reader.getSchema());
                        // TODO change actualOut back to outputStream once the bug mentioned in the TODO below is fixed
                        try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, actualOut, originalAttributes)) {
                            writer.beginRecordSet();
                            final WriteResult writeResult = writer.finishRecordSet();
                            attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                            attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                            attributes.putAll(writeResult.getAttributes());
                            recordCount.set(writeResult.getRecordCount());
                        }
                        return;
                    }

                    final RecordSchema writeSchema = writerFactory.getSchema(originalAttributes, firstRecord.getSchema());
                    // TODO change actualOut back to outputStream once the bug mentioned in the TODO below is fixed
                    try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, actualOut, originalAttributes)) {
                        writer.beginRecordSet();
                        writer.write(firstRecord);

                        Record record;
                        while ((record = reader.nextRecord()) != null) {
                            writer.write(record);
                        }

                        final WriteResult writeResult = writer.finishRecordSet();
                        attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                        attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                        attributes.putAll(writeResult.getAttributes());
                        recordCount.set(writeResult.getRecordCount());
                    }

                    // TODO : delete this entire if/else when the bug in the XMLReader component that
                    // causes empty XML elements to be ignored is fixed.
                    if (!isUnparse()) {
                        ByteArrayOutputStream baos = (ByteArrayOutputStream) actualOut;
                        String content = new String(baos.toByteArray());
                        if (content.startsWith("<?xml version=\"1.0\" ?>")) {
                            content = content.replaceAll("<(\\S+)></(\\1)>", "<$1>\u200B</$1>");
                            outputStream.write(content.getBytes(StandardCharsets.UTF_8));
                            outputStream.flush();
                        } else {
                            outputStream.write(baos.toByteArray());
                        }
                    }
                } catch (final SchemaNotFoundException e) {
                    throw new ProcessException("Schema not Found: " + e.getLocalizedMessage(), e);
                } catch (final MalformedRecordException e) {
                    throw new ProcessException("Could not parse incoming data", e);
                }
            });
        } catch (final Exception e) {
            getLogger().error("Failed to process {} due to {}; will route to failure", new Object[] {flowFile, e});
            /* Since we are wrapping the exceptions above there should always be a cause
             * but it's possible it might not have a message. This handles that by logging
             * the name of the class thrown.
             */
            Throwable c = e.getCause();
            if (c != null) {
                session.putAttribute(flowFile, "record.error.message", (c.getLocalizedMessage() != null) ? c.getLocalizedMessage() : c.getClass().getCanonicalName() + " Thrown");
            } else {
                session.putAttribute(flowFile, "record.error.message", e.getClass().getCanonicalName() + " Thrown");
            }
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        flowFile = session.putAllAttributes(flowFile, attributes);
        if (recordCount.get() == 0){
            session.remove(flowFile);
        } else {
            session.transfer(flowFile, REL_SUCCESS);
        }

        final int count = recordCount.get();
        session.adjustCounter("Records Processed", count, false);
        getLogger().info("Successfully converted {} records for {}", new Object[] {count, flowFile});
    }

}
