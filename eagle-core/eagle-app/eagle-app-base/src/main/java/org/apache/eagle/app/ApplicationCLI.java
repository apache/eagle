/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.app;

/**
 * Usage: java org.apache.eagle.app.ApplicationCLI [APPLICATION_CLASS] [APPLICATION_OPTIONS: -D Key=Value]
 */
public class ApplicationCLI {
    /**
     * @param args
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        if(args.length < 1){
            System.err.println("Usage: java "+ApplicationCLI.class.getName()+" [APPLICATION_CLASS] [APPLICATION_OPTIONS: -D Key=Value]");
            System.exit(1);
        } else {
            String appClassName = args[0];
            Class<? extends ApplicationTool> appClass = (Class<? extends ApplicationTool>) Class.forName(appClassName);
            String[] appArgs = new String[args.length-1];
            System.arraycopy(args, 1, appArgs, 0, args.length - 1);
            appClass.newInstance().run(appArgs);
        }
    }
}