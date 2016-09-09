/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.jpm.spark.history.crawl;

import org.apache.eagle.jpm.util.SparkJobTagName;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class SparkFilesystemInputStreamReaderImpl implements JHFInputStreamReader {

    private String site;
    private SparkApplicationInfo app;


    public SparkFilesystemInputStreamReaderImpl(String site, SparkApplicationInfo app) {
        this.site = site;
        this.app = app;
    }

    @Override
    public void read(InputStream is) throws Exception {
        Map<String, String> baseTags = new HashMap<>();
        baseTags.put(SparkJobTagName.SITE.toString(), site);
        baseTags.put(SparkJobTagName.SPARK_QUEUE.toString(), app.getQueue());
        JHFParserBase parser = new JHFSparkParser(new JHFSparkEventReader(baseTags, this.app));
        parser.parse(is);
    }

    public static void main(String[] args) throws Exception {
        SparkFilesystemInputStreamReaderImpl impl = new SparkFilesystemInputStreamReaderImpl("apollo-phx", new SparkApplicationInfo());
        impl.read(new FileInputStream(new File("E:\\eagle\\application_1459803563374_535667_1")));
    }

}
