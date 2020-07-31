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

import org.apache.daffodil.japi.DataProcessor;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.OutputStream;
import java.util.Map;

@EventDriven
@SideEffectFree
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"xml", "json", "daffodil", "dfdl", "schema", "xsd"})
@CapabilityDescription("Use Daffodil and a user-specified DFDL schema to unparse a Daffodil Infoset into data")
public class DaffodilUnparse extends AbstractDaffodilProcessor {
    @Override
    protected boolean isUnparse() {
        return true;
    }

    public static class DFDLWriterFactory extends AbstractControllerService implements RecordSetWriterFactory {
        private final DataProcessor configuredProcessor;
        private final RecordSchema originalSchema;
        private final boolean streamMode;

        public DFDLWriterFactory(DataProcessor configuredProcessor, RecordSchema originalSchema, boolean streamMode) {
            this.configuredProcessor = configuredProcessor;
            this.originalSchema = originalSchema;
            this.streamMode = streamMode;
        }

        @Override
        public RecordSchema getSchema(Map<String, String> variables, RecordSchema readSchema) {
            return readSchema;
        }

        @Override
        public RecordSetWriter createWriter(ComponentLog logger, RecordSchema schema,
                                            OutputStream out, Map<String, String> variables) {
            return new DaffodilRecordWriter(configuredProcessor, out, originalSchema, streamMode, logger);
        }
    }

}
