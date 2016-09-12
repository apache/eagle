/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.eagle.jpm.mr.history.parser;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.mapreduce.jobhistory.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

public class JHFMRVer2Parser implements JHFParserBase {
    private static final Logger logger = LoggerFactory.getLogger(JHFMRVer2Parser.class);
    private JHFMRVer2EventReader reader;

    public JHFMRVer2Parser(JHFMRVer2EventReader reader) {
        this.reader = reader;
    }

    @SuppressWarnings( {"rawtypes", "deprecation"})
    @Override
    public void parse(InputStream is) throws Exception {
        int eventCtr = 0;
        try {
            final long start = System.currentTimeMillis();
            DataInputStream in = new DataInputStream(is);
            String version = in.readLine();
            if (!"Avro-Json".equals(version)) {
                throw new IOException("Incompatible event log version: " + version);
            }

            Schema schema = Schema.parse(in.readLine());
            SpecificDatumReader datumReader = new SpecificDatumReader(schema);
            JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, in);

            Event wrapper;
            while ((wrapper = getNextEvent(datumReader, decoder)) != null) {
                ++eventCtr;
                reader.handleEvent(wrapper);
            }
            reader.parseConfiguration();
            // don't need put to finally as it's a kind of flushing data
            reader.close();
            logger.info("reader used " + (System.currentTimeMillis() - start) + "ms");
        } catch (Exception ioe) {
            logger.error("Caught exception parsing history file after " + eventCtr + " events", ioe);
            throw ioe;
        } finally {
            if (is != null) {
                is.close();
            }
        }
    }

    @SuppressWarnings( {"rawtypes", "unchecked"})
    public Event getNextEvent(DatumReader datumReader, JsonDecoder decoder) throws Exception {
        Event wrapper;
        try {
            wrapper = (Event) datumReader.read(null, decoder);
        } catch (java.io.EOFException e) {            // at EOF
            return null;
        }
        return wrapper;
    }

}
