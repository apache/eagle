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
package org.apache.eagle.app.test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.eagle.app.module.ApplicationExtensionLoader;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AppUnitTestRunner extends BlockJUnit4ClassRunner {
    private final Injector injector;
    public AppUnitTestRunner(Class<?> klass) throws InitializationError {
        super(klass);
        injector = createInjectorFor(getModulesFor(klass));
    }

    @Override
    protected Object createTest() throws Exception {
        final Object obj = super.createTest();
        injector.injectMembers(this);
        this.injector.injectMembers(obj);
        return obj;
    }

    /**
     * Create a Guice Injector for the class under test.
     * @param classes Guice Modules
     * @return A Guice Injector instance.
     * @throws InitializationError If couldn't instantiate a module.
     */
    private Injector createInjectorFor(final Class<?>[] classes)
            throws InitializationError {
        final List<Module> modules = new ArrayList<>();

        AppTestGuiceModule testGuiceModule = new AppTestGuiceModule();

        // Add default modules
        modules.add(testGuiceModule);

        if(classes!= null) {
            for (final Class<?> module : Arrays.asList(classes)) {
                try {
                    modules.add((Module) module.newInstance());
                } catch (final ReflectiveOperationException exception) {
                    throw new InitializationError(exception);
                }
            }
        }
        return Guice.createInjector(modules);
    }

    /**
     * Get the list of Guice Modules request by GuiceModules annotation in the
     * class under test.
     * @param klass Class under test.
     * @return A Class Array of Guice Modules required by this class.
     * @throws InitializationError If the annotation is not present.
     */
    private Class<?>[] getModulesFor(final Class<?> klass)
            throws InitializationError {
        final Modules annotation = klass.getAnnotation(Modules.class);
        if (annotation == null) {
            return null;
        }
        return annotation.value();
    }
}