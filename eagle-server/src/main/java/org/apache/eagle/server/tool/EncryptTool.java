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
package org.apache.eagle.server.tool;

import org.apache.eagle.server.ServerMain;
import org.apache.eagle.server.security.encrypt.EncryptorFactory;
import org.apache.eagle.server.security.encrypt.PasswordEncryptor;

public class EncryptTool implements Tool {

    private static final String MISSING_TEXT =
        "Error: require text to encrypt\n"
        + "Usage: java " + ServerMain.class.getName() + " encrypt [text to encrypt]";

    @Override
    public void execute(String[] args) {
        if (args.length < 2) {
            System.err.println(MISSING_TEXT);
        } else {
            String textToEncrypt = args[1];
            PasswordEncryptor encryptor = EncryptorFactory.getPasswordEncryptor();
            String encryptedText = encryptor.encryptPassword(textToEncrypt);
            System.out.println("Original: " + textToEncrypt);
            System.out.println("Encrypted: " + encryptedText);
            System.exit(0);
        }
    }
}
