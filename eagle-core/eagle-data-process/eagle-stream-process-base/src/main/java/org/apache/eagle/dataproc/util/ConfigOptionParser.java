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
package org.apache.eagle.dataproc.util;

import org.apache.commons.cli.*;

import java.util.HashMap;
import java.util.Map;


public class ConfigOptionParser extends AbstractConfigOptionParser {
    private final static String CONFIG_OPT_FLAG = "D";

    @Override
    protected Parser parser() {
        return new BasicParser();
    }

    @Override
    protected Options options() {
        Options options = new Options();
        options.addOption(CONFIG_OPT_FLAG, true, "Config properties in format of \"-D key=value\"");
        return options;
    }

    @Override
    public Map<String,String> parseConfig(String[] arguments) throws ParseException {
        CommandLine cmd = parse(arguments);

        Map<String,String> result = new HashMap<>();
        if(cmd.hasOption(CONFIG_OPT_FLAG)){
            String[] values = cmd.getOptionValues(CONFIG_OPT_FLAG);
            for(String value:values){
                int eqIndex = value.indexOf("=");
                if(eqIndex>0 && eqIndex<value.length()){
                    String k = value.substring(0,eqIndex);
                    String v = value.substring(eqIndex+1,value.length());
                    if(result.containsKey(k)){
                        throw new ParseException("Duplicated "+CONFIG_OPT_FLAG+" "+value);
                    }else{
                        result.put(k,v);
                    }
                }else{
                    throw new ParseException("Invalid format: -"+CONFIG_OPT_FLAG+" "+value+", required: -"+CONFIG_OPT_FLAG+" key=value");
                }
            }
        }
        return result;
    }
}