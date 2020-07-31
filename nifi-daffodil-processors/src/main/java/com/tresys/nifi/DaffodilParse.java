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
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.InputStream;
import java.util.Map;

@EventDriven
@SideEffectFree
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"xml", "json", "daffodil", "dfdl", "schema", "xsd"})
@CapabilityDescription("Use Daffodil and a user-specified DFDL schema to transform data to an infoset into Records")
public class DaffodilParse extends AbstractDaffodilProcessor {

    @Override
    protected boolean isUnparse() {
        return false;
    }

    public static class DFDLReaderFactory extends AbstractControllerService implements RecordReaderFactory {
        private final RecordSchema schema;
        private final InputStream inputStream;
        private final DataProcessor configuredProcessor;
        private final boolean streamMode;

        public DFDLReaderFactory(RecordSchema schema, InputStream inputStream,
                                 DataProcessor configuredProcessor, boolean streamMode) {
            this.schema = schema;
            this.inputStream = inputStream;
            this.configuredProcessor = configuredProcessor;
            this.streamMode = streamMode;
        }

        @Override
        public RecordReader createRecordReader(Map<String, String> variables,
                                               InputStream in, long inputLength, ComponentLog logger) {
            return new DaffodilRecordReader(schema, inputStream, configuredProcessor, streamMode, logger);
        }

    }

}
