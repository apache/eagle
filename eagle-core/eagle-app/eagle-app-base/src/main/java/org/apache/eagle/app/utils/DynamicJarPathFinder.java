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
package org.apache.eagle.app.utils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;

/**
 * http://stackoverflow.com/questions/1983839/determine-which-jar-file-a-class-is-from
 * https://github.com/rzwitserloot/lombok.patcher/blob/master/src/lombok/patcher/inject/LiveInjector.java
 */
public class DynamicJarPathFinder {
    private final static Logger LOG = LoggerFactory.getLogger(DynamicJarPathFinder.class);
    /**
     * If the provided class has been loaded from a jar file that is on the local file system, will find the absolute path to that jar file.
     *
     * @param context The jar file that contained the class file that represents this class will be found. Specify {@code null} to let {@code LiveInjector}
     *                find its own jar.
     * @throws IllegalStateException If the specified class was loaded from a directory or in some other way (such as via HTTP, from a database, or some
     *                               other custom classloading device).
     */
    public static String findPathJar(Class<?> context) throws IllegalStateException {
        if (context == null) context = DynamicJarPathFinder.class;
        String rawName = context.getName();
        String classFileName;
    /* rawName is something like package.name.ContainingClass$ClassName. We need to turn this into ContainingClass$ClassName.class. */ {
            int idx = rawName.lastIndexOf('.');
            classFileName = (idx == -1 ? rawName : rawName.substring(idx+1)) + ".class";
        }

        String uri = context.getResource(classFileName).toString();
        if (uri.startsWith("file:")) {
            throw new IllegalStateException("This class has been loaded from a directory and not from a jar file.");
        }
        if (!uri.startsWith("jar:file:")) {
            int idx = uri.indexOf(':');
            String protocol = idx == -1 ? "(unknown)" : uri.substring(0, idx);
            throw new IllegalStateException("This class has been loaded remotely via the " + protocol +
                    " protocol. Only loading from a jar on the local file system is supported.");
        }

        int idx = uri.indexOf('!');
        //As far as I know, the if statement below can't ever trigger, so it's more of a sanity check thing.
        if (idx == -1) throw new IllegalStateException("You appear to have loaded this class from a local jar file, but I can't make sense of the URL!");

        try {
            String fileName = URLDecoder.decode(uri.substring("jar:file:".length(), idx), Charset.defaultCharset().name());
            return new File(fileName).getAbsolutePath();
        } catch (UnsupportedEncodingException e) {
            throw new InternalError("default charset doesn't exist. Your VM is borked.");
        }
    }

    /**
     * Similar to JarPathFinder, but not make sure the path must valid jar.
     *
     * @see DynamicJarPathFinder#findPathJar(Class)
     * @return the class path contains the context class
     */
    public static String findPath(Class<?> context) throws IllegalStateException {
        if (context == null) context = DynamicJarPathFinder.class;
        String rawName = context.getName();
        String classFileName;
    /* rawName is something like package.name.ContainingClass$ClassName. We need to turn this into ContainingClass$ClassName.class. */ {
            int idx = rawName.lastIndexOf('.');
            classFileName = (idx == -1 ? rawName : rawName.substring(idx+1)) + ".class";
        }

        String uri = context.getResource(classFileName).toString();
        if (uri.startsWith("file:")) {
            LOG.warn("This class has been loaded from a directory and not from a jar file: {}",uri);
            String fileName = null;
            try {
                fileName = URLDecoder.decode(uri.substring("file:".length(), uri.length()), Charset.defaultCharset().name());
                return new File(fileName).getAbsolutePath();
            } catch (UnsupportedEncodingException e) {
                throw new InternalError("default charset doesn't exist. Your VM is borked.");
            }
        }

        if (!uri.startsWith("jar:file:")) {
            int idx = uri.indexOf(':');
            String protocol = idx == -1 ? "(unknown)" : uri.substring(0, idx);
            throw new IllegalStateException("This class has been loaded remotely via the " + protocol +
                    " protocol. Only loading from a jar on the local file system is supported.");
        }

        int idx = uri.indexOf('!');
        //As far as I know, the if statement below can't ever trigger, so it's more of a sanity check thing.
        if (idx == -1) {
            throw new IllegalStateException("You appear to have loaded this class from a local jar file, but I can't make sense of the URL!");
        }

        try {
            String fileName = URLDecoder.decode(uri.substring("jar:file:".length(), idx), Charset.defaultCharset().name());
            return new File(fileName).getAbsolutePath();
        } catch (UnsupportedEncodingException e) {
            throw new InternalError("default charset doesn't exist. Your VM is borked.");
        }
    }
}
