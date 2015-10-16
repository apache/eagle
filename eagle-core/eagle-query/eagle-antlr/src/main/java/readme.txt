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
1. Add dependency for antlr 4.0 runtime library
	<dependency>
      <groupId>org.antlr</groupId>
      <artifactId>antlr4-runtime</artifactId>
      <version>4.0</version>
    </dependency>

2. Compile the grammar file
java -jar antlr\antlr-4.0-complete.jar -package eagle.query.antlr.generated -o eagle\query\antlr\generated EagleFilter.g4

3. Test boolean expression parsing
3.1 Download antlr 4.0 complete library
http://www.antlr.org/download/antlr-4.0-complete.jar
3.2 Test
java -cp  antlr\antlr-4.0-complete.jar;./  org.antlr.v4.runtime.misc.TestRig eagle.query.antlr.generated.EagleFilter filter -gui <path>