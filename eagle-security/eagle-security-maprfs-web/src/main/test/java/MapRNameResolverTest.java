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


import org.apache.eagle.service.security.hdfs.rest.MapRNameResolver;

public class MapRNameResolverTest {

    public static void main(String[] args){
        MapRNameResolver resolver = new MapRNameResolver();
        String name = "/test05";
        String id = "";
        try{
            //id = resolver.getFid(name);
        }catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("name to Id: " + id);
    }

}