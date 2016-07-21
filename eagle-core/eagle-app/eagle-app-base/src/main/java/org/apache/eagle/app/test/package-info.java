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
 *
 *
 *
 */

/**
 * <h1>How to test application ?</h1>
 *
 * <h2>Option 1: Test with AppTestRunner</h2>
 * <pre>
 * @RunWith(AppTestRunner.class)
 * public class ExampleApplicationTest {
 *     @Inject
 *     private ApplicationResource applicationResource;
 * }
 * </pre>
 *
 * <h2>Option 2: Manually create injector</h2>
 * <pre>
 * public class ExampleApplicationTest {
 *     @Inject
 *     private ApplicationResource applicationResource;
 *
 *     @Before
 *     public void setUp(){
 *         Guice.createInjector(new AppTestModule()).injector.injectMembers(this);
 *     }
 * }
 * </pre>
 */
package org.apache.eagle.app.test;