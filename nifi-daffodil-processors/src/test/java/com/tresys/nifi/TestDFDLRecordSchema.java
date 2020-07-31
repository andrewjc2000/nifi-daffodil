package com.tresys.nifi;

import org.apache.daffodil.japi.Daffodil;
import org.apache.daffodil.japi.ProcessorFactory;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class TestDFDLRecordSchema {

    private RecordSchema rootSchema;
    private void setup(String pathToDFDLSchema) throws IOException {
        File schemaFile = new File(pathToDFDLSchema);
        assertTrue(schemaFile.exists());
        RecordWalker walker = new RecordWalker();
        ProcessorFactory factory = Daffodil.compiler().compileFile(schemaFile);
        assertFalse(factory.getDiagnostics().toString(), factory.isError());
        walker.walkFromRoot(factory.experimental().rootView());
        RecordSchema obtainedSchema = walker.getResult();
        assertEquals(1, obtainedSchema.getFieldCount());
        assertEquals("root", obtainedSchema.getField(0).getFieldName());
        rootSchema = ((RecordDataType) obtainedSchema.getField(0).getDataType()).getChildSchema();
    }

    @Test
    public void testChoiceWithGroupRef() throws IOException {
        setup("src/test/resources/TestDFDLRecordSchema/choiceWithGroupRef.dfdl.xsd");
        assertEquals(1, rootSchema.getFieldCount());
        assertEquals("CHOICE[]", rootSchema.getField(0).getFieldName());
        assertTrue(rootSchema.getField(0).getDataType() instanceof ChoiceDataType);
        ChoiceDataType anonChoice = (ChoiceDataType) rootSchema.getField(0).getDataType();
        assertEquals(2, anonChoice.getPossibleSubTypes().size());
        assertTrue(anonChoice.getPossibleSubTypes().get(0) instanceof RecordDataType);
        assertTrue(anonChoice.getPossibleSubTypes().get(1) instanceof RecordDataType);
        RecordDataType field1 = (RecordDataType) anonChoice.getPossibleSubTypes().get(0);
        RecordDataType field2 = (RecordDataType) anonChoice.getPossibleSubTypes().get(1);
        assertEquals("field1", field1.getChildSchema().getField(0).getFieldName());
        assertEquals("field2", field2.getChildSchema().getField(0).getFieldName());
    }

    @Test
    public void testOptionalField() throws IOException {
        setup("src/test/resources/TestDFDLRecordSchema/optionalField.dfdl.xsd");
        assertEquals(2, rootSchema.getFieldCount());
        assertEquals("imOptional", rootSchema.getField(0).getFieldName());
        assertTrue(rootSchema.getField(0) instanceof OptionalRecordField);
        assertEquals("imRequired", rootSchema.getField(1).getFieldName());
        assertFalse(rootSchema.getField(1) instanceof OptionalRecordField);
    }

}
