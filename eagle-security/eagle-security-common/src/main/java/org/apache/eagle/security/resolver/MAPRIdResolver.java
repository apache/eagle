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
package org.apache.eagle.security.resolver;

import com.mapr.fs.clicommands.MapRCliCommands;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class MAPRIdResolver {
    private final static Logger LOG = LoggerFactory.getLogger(MAPRIdResolver.class);
    private static MapRCliCommands mprcmd;

    private String run(String[] cmds) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        PrintStream old = System.out;
        System.setOut(ps);
        mprcmd.run(cmds);
        System.out.flush();
        System.setOut(old);
        return baos.toString().trim();
    }

    public MAPRIdResolver(){
        Configuration conf = new Configuration();
        mprcmd = new MapRCliCommands(conf);
    }

/**
 * Todo: currently, MAPRIdResolver make use of MapR's terminal commands "lsfid", which takes too much time.
 * Will check if we can find out the algorithm for mapping name to ids.
 * */
    public  String fidToName(String id) throws Exception {
        String name = "";
        String cmds[] = {"-lsfid",id};
        String results = run(cmds);
        Pattern pattern = Pattern.compile("\\s(/.*)\\s");
        Matcher matcher = pattern.matcher(results);
        if (matcher.find()){
            name = matcher.group(1);
        }
        return name;
    }

    public  String nameToFid(String name) throws Exception {
        String id = "";
        name = "maprfs://"+name;
        String cmds[] = {"-ls",name};
        String results = run(cmds);
        Pattern pattern = Pattern.compile("\\sp\\s([\\d\\.]+)\\s");
        Matcher matcher = pattern.matcher(results);
        if (matcher.find()){
            id = matcher.group(1);
        }
        return id;
    }



    //Todo: user id conversion
    // case 1. userId is stored in local machine
    // case 2. userId is shared within the cluster
}

