/*
 * Copyright 2018 Tresys Technology, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.HashMap;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.xml.XMLReader;
import org.apache.nifi.xml.XMLRecordSetWriter;

import org.junit.Test;

public class TestDaffodilProcessor {

    @Test
    public void testDFDLSchemaNotFound() {
        final TestRunner testRunner = TestRunners.newTestRunner(DaffodilParse.class);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "/does/not/exist.dfdl.xsd");
        testRunner.assertNotValid();
    }

    private enum ControllerOption {
        XML_READER,
        XML_WRITER,
        JSON_READER,
        JSON_WRITER
    }

    private TestRunner setupRunner(ControllerOption option, boolean expressionLanguage, boolean safeXMLSetup) throws InitializationException {
        TestRunner testRunner;
        switch (option) {
            case XML_READER:
                testRunner = TestRunners.newTestRunner(DaffodilUnparse.class);
                ControllerService xmlReader = new XMLReader();
                testRunner.addControllerService("xmlReader", xmlReader);
                if (safeXMLSetup) {
                    testRunner.setProperty(xmlReader, XMLReader.RECORD_FORMAT, XMLReader.RECORD_ARRAY);
                }
                testRunner.enableControllerService(xmlReader);
                testRunner.setProperty(DaffodilParse.RECORD_READER, "xmlReader");
                break;
            case XML_WRITER:
                testRunner = TestRunners.newTestRunner(DaffodilParse.class);
                ControllerService xmlWriter = new XMLRecordSetWriter();
                testRunner.addControllerService("xmlWriter", xmlWriter);
                if (safeXMLSetup) {
                    testRunner.setProperty(xmlWriter, XMLRecordSetWriter.ROOT_TAG_NAME, "Record");
                }
                testRunner.enableControllerService(xmlWriter);
                testRunner.setProperty(DaffodilParse.RECORD_WRITER, "xmlWriter");
                break;
            case JSON_READER:
                testRunner = TestRunners.newTestRunner(DaffodilUnparse.class);
                ControllerService jsonReader = new JsonTreeReader();
                testRunner.addControllerService("jsonReader", jsonReader);
                testRunner.enableControllerService(jsonReader);
                testRunner.setProperty(DaffodilParse.RECORD_READER, "jsonReader");
                break;
            default:
                testRunner = TestRunners.newTestRunner(DaffodilParse.class);
                ControllerService jsonWriter = new JsonRecordSetWriter();
                testRunner.addControllerService("jsonWriter", jsonWriter);
                testRunner.enableControllerService(jsonWriter);
                testRunner.setProperty(DaffodilParse.RECORD_WRITER, "jsonWriter");
        }
        testRunner.setValidateExpressionUsage(expressionLanguage);
        return testRunner;
    }

    private TestRunner setupRunner(ControllerOption option, boolean expressionLanguage) throws InitializationException {
        return setupRunner(option, expressionLanguage, false);
    }

    @Test
    public void testDFDLSchemaNotValid() throws IOException, InitializationException {
        final TestRunner testRunner = setupRunner(ControllerOption.XML_WRITER, false);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv-invalid.dfdl.xsd");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_FAILURE);
        final MockFlowFile original = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_FAILURE).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv")));
        original.assertContentEquals(expectedContent);
    }

    @Test
    public void testParseCSV() throws IOException, InitializationException {
        final TestRunner testRunner = setupRunner(ControllerOption.XML_WRITER, false);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testParseCSVFail() throws IOException, InitializationException {
        final TestRunner testRunner = setupRunner(ControllerOption.XML_WRITER, false);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        // trying to parse the XML file will fail, expects CSV data
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_FAILURE);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_FAILURE).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testUnparseCSV() throws IOException, InitializationException {
        final TestRunner testRunner = setupRunner(ControllerOption.XML_READER, false);
        testRunner.setProperty(DaffodilUnparse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilUnparse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilUnparse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv")));
        infoset.assertContentEquals(expectedContent.replace("\r\n", "\n"));
    }

    @Test
    public void testUnparseCSVFail() throws IOException, InitializationException {
        final TestRunner testRunner = setupRunner(ControllerOption.XML_READER, false);
        testRunner.setProperty(DaffodilUnparse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        // trying to unparse CSV will fail, expectes an XML infoset
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilUnparse.REL_FAILURE);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilUnparse.REL_FAILURE).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testExpressionLanguage() throws IOException, InitializationException {
        final TestRunner testRunner = setupRunner(ControllerOption.XML_WRITER, true);
        testRunner.setProperty(DaffodilUnparse.DFDL_SCHEMA_FILE, "${dfdl.schema}");
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("dfdl.schema", "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv"), attributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testNoCache() throws IOException, InitializationException {
        final TestRunner testRunner = setupRunner(ControllerOption.XML_WRITER, false);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        testRunner.setProperty(DaffodilParse.CACHE_SIZE, "0");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilUnparse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilUnparse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testParseCSVJson() throws IOException, InitializationException {
        final TestRunner testRunner = setupRunner(ControllerOption.JSON_WRITER, false);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.json")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testUnparseCSVJson() throws IOException, InitializationException {
        final TestRunner testRunner = setupRunner(ControllerOption.JSON_READER, false);
        testRunner.setProperty(DaffodilUnparse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.json"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilUnparse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilUnparse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv")));
        infoset.assertContentEquals(expectedContent.replace("\r\n", "\n"));
    }

    @Test
    public void testUnparseCSVAttributeJSON() throws IOException, InitializationException {
        final TestRunner testRunner = setupRunner(ControllerOption.JSON_READER, false);
        testRunner.setProperty(DaffodilUnparse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.json"), new HashMap<>());
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilUnparse.REL_SUCCESS);
        final MockFlowFile csvFile = testRunner.getFlowFilesForRelationship(DaffodilUnparse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv")));
        csvFile.assertContentEquals(expectedContent.replace("\r\n", "\n"));
    }

    @Test
    public void testUnparseCSVAttributeXML() throws IOException, InitializationException {
        final TestRunner testRunner = setupRunner(ControllerOption.XML_READER, false);
        testRunner.setProperty(DaffodilUnparse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv.xml"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilUnparse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilUnparse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv")));
        infoset.assertContentEquals(expectedContent.replace("\r\n", "\n"));
    }

    @Test
    public void testParseLeftOverData() throws IOException, InitializationException {
        final TestRunner testRunner = setupRunner(ControllerOption.XML_WRITER, false);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/bitlength.dfdl.xsd");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/leftover.bin"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_FAILURE);
        final MockFlowFile original = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_FAILURE).get(0);
        final byte[] expectedContent = Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/leftover.bin"));
        original.assertContentEquals(expectedContent);
    }

    @Test
    public void testParseNoLeftOverData() throws IOException, InitializationException {
        final TestRunner testRunner = setupRunner(ControllerOption.XML_WRITER, false);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/bitlength.dfdl.xsd");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/noleftover.bin"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_SUCCESS).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/noleftover.bin.xml")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testParseCSVValidationLimited() throws IOException, InitializationException {
        final TestRunner testRunner = setupRunner(ControllerOption.XML_WRITER, false);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        testRunner.setProperty(DaffodilParse.VALIDATION_MODE, DaffodilParse.LIMITED_VALUE);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_FAILURE);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_FAILURE).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testParseCSVValidationFull() throws IOException, InitializationException {
        final TestRunner testRunner = setupRunner(ControllerOption.XML_WRITER, false);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/csv.dfdl.xsd");
        testRunner.setProperty(DaffodilParse.VALIDATION_MODE, DaffodilParse.FULL_VALUE);
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_FAILURE);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_FAILURE).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/tokens.csv")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testTunableParameters() throws IOException, InitializationException {
        final TestRunner testRunner = setupRunner(ControllerOption.XML_WRITER, false);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/fixed-list.dfdl.xsd");
        testRunner.setProperty(DaffodilParse.TUNABLE_LIST, "maxOccursBounds=5");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/simpleList.txt"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_FAILURE);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_FAILURE).get(0);
        final String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/simpleList.txt")));
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testExternalVariables() throws IOException, InitializationException {
        final TestRunner testRunner = setupRunner(ControllerOption.JSON_WRITER, false);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/fixed-list.dfdl.xsd");
        testRunner.setProperty(DaffodilParse.EXTERNAL_VARIABLES_LIST, "valueToOverride=1");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/simpleList.txt"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_SUCCESS).get(0);
        String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/simpleList-extVars.txt.json")));
        expectedContent = expectedContent.replaceAll("\"\\$expectedValue\"", "1");
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testConfigFile() throws IOException, InitializationException {
        final TestRunner testRunner = setupRunner(ControllerOption.JSON_WRITER, false);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/fixed-list.dfdl.xsd");
        testRunner.setProperty(DaffodilParse.CONFIG_FILE, "src/test/resources/TestDaffodilProcessor/testConfig.xml");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/simpleList.txt"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_SUCCESS);
        final MockFlowFile infoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_SUCCESS).get(0);
        String expectedContent = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/simpleList-extVars.txt.json")));
        expectedContent = expectedContent.replaceAll("\"\\$expectedValue\"", "2");
        infoset.assertContentEquals(expectedContent);
    }

    @Test
    public void testStreamModeForJsonParse() throws IOException, InitializationException {
        final TestRunner testRunner = setupRunner(ControllerOption.JSON_WRITER, false);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/fixed-list.dfdl.xsd");
        testRunner.setProperty(DaffodilParse.STREAM_MODE, "true");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/simpleList-extraData.txt"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_SUCCESS, 1);
        final MockFlowFile firstInfoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_SUCCESS).get(0);
        String firstExpected = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/simpleList-extraData.txt.json")));
        firstInfoset.assertContentEquals(firstExpected);
    }

    @Test
    public void testStreamModeForJsonUnparse() throws IOException, InitializationException {
        final TestRunner testRunner = setupRunner(ControllerOption.JSON_READER, false);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/fixed-list.dfdl.xsd");
        testRunner.setProperty(DaffodilParse.STREAM_MODE, "true");
        testRunner.enqueue(Paths.get("src/test/resources/TestDaffodilProcessor/simpleList-extraData.txt.json"));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_SUCCESS, 1);
        final MockFlowFile firstInfoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_SUCCESS).get(0);
        String firstExpected = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/simpleList-extraData.txt")));
        firstInfoset.assertContentEquals(firstExpected);
    }

    @Test
    public void testStreamModeForXMLParseUnparse() throws IOException, InitializationException {
        final TestRunner testRunner = setupRunner(ControllerOption.XML_WRITER, false, true);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/fixed-list.dfdl.xsd");
        testRunner.setProperty(DaffodilParse.STREAM_MODE, "true");
        testRunner.enqueue(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/simpleList-extraData.txt")));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_SUCCESS, 1);
        final MockFlowFile infosetBytes = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_SUCCESS).get(0);
        final TestRunner testRunner2 = setupRunner(ControllerOption.XML_READER, false, true);
        testRunner2.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/fixed-list.dfdl.xsd");
        testRunner2.setProperty(DaffodilParse.STREAM_MODE, "true");
        testRunner2.enqueue(infosetBytes);
        testRunner2.run();
        testRunner2.assertAllFlowFilesTransferred(DaffodilParse.REL_SUCCESS, 1);
        final MockFlowFile unparsedData = testRunner2.getFlowFilesForRelationship(DaffodilParse.REL_SUCCESS).get(0);
        final String expected = new String(Files.readAllBytes(Paths.get("src/test/resources/TestDaffodilProcessor/simpleList-extraData.txt")));
        // TODO change this back to just Equals(expected) once bug in XMLReader that ignores empty elements is fixed
        unparsedData.assertContentEquals(expected.replace("exactly,", "exactly"));
    }

    @Test
    public void testNestedChoicesValidCase() throws InitializationException {
        final TestRunner testRunner = setupRunner(ControllerOption.JSON_WRITER, false);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/nestedChoices.dfdl.xsd");
        testRunner.enqueue("2");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_SUCCESS, 1);
        final MockFlowFile firstInfoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_SUCCESS).get(0);
        firstInfoset.assertContentEquals("[{\"root\":{\"B\":2}}]");
    }

    @Test
    public void testNestedChoicesDefaultCase() throws InitializationException {
        final TestRunner testRunner = setupRunner(ControllerOption.JSON_WRITER, false);
        testRunner.setProperty(DaffodilParse.DFDL_SCHEMA_FILE, "src/test/resources/TestDaffodilProcessor/nestedChoices.dfdl.xsd");
        testRunner.enqueue("4");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(DaffodilParse.REL_SUCCESS, 1);
        final MockFlowFile firstInfoset = testRunner.getFlowFilesForRelationship(DaffodilParse.REL_SUCCESS).get(0);
        firstInfoset.assertContentEquals("[{\"root\":{\"D\":4}}]");
    }

}