/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.app.stream;

import org.apache.eagle.app.environment.builder.CEPFunction;
import org.apache.eagle.app.environment.builder.Collector;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class CEPFunctionTest {
    @Test @Ignore("TODO: Not implement yet")
    public void testSiddhiFunction() {
        CEPFunction function = new CEPFunction(
            "define stream inputStream (name string, value double);\n "
                + "from inputStream#window.timeBatch( 1 min ) \n" +
                "select name, avg(value) as avgValue\n" +
                "group by name \n" +
                "insert into outputStream ",
            "inputStream","outputStream");
        Collector collector = new Collector() {
            @Override
            public void collect(Object key, Map event) {

            }
        };
        function.open(collector);
        function.transform(new HashMap<String,Object>() {{
            put("name","cpu.usage");
            put("value", 0.98);
        }});
        function.close();
    }
}
