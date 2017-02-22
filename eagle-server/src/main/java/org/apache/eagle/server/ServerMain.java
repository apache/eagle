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
package org.apache.eagle.server;

import org.apache.eagle.common.Version;
import org.apache.eagle.server.tool.EncryptTool;

import java.util.Date;

public class ServerMain {
    private static final String USAGE =
        "Usage: java " + ServerMain.class.getName() + " command [options] \n"
        + "where options include: \n"
        + "\tserver\t[path to configuration]\n"
        + "\tencrypt\t[text to encrypt]\n";

    public static void main(String[] args) {
        if (args.length > 1) {
            String cmd = args[0];

            switch (cmd) {
                case "server":
                    System.out.println(
                        "\nApache Eagle™ v" + Version.version + ": "
                            + "built with git revision " + Version.gitRevision + " by " + Version.userName + " on " + new Date(Long.parseLong(Version.timestamp))
                    );

                    System.out.println("\nStarting Eagle Server ...\n");
                    try {
                        new ServerApplication().run(args);
                    } catch (Exception e) {
                        System.err.println("Oops, got error to start eagle server: " + e.getMessage());
                        e.printStackTrace();
                        System.exit(1);
                    }
                    break;
                case "encrypt":
                    new EncryptTool().execute(args);
                    break;
                default:
                    System.err.println("Invalid command " + cmd);
                    System.err.print(USAGE);
                    System.exit(2);
            }
        } else {
            System.err.print(USAGE);
            System.exit(1);
        }
    }
}