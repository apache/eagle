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

package org.apache.eagle.datastream.utils

import scala.reflect.api
import scala.reflect.runtime.{universe => ru}

/**
 * @since  12/7/15
 */
object Reflections{
  private val UNIT_CLASS = classOf[Unit]
  private val UNIT_TYPE_TAG = ru.typeTag[Unit]

  /**
   * Class to TypeTag
   * @param clazz class
   * @tparam T Type T
   * @return
   */
  def typeTag[T](clazz:Class[T]):ru.TypeTag[T]={
    if(clazz == null){
      null
    }else if(clazz == UNIT_CLASS) {
      UNIT_TYPE_TAG.asInstanceOf[ru.TypeTag[T]]
    } else {
      val mirror = ru.runtimeMirror(clazz.getClassLoader)
      val sym = mirror.staticClass(clazz.getCanonicalName)
      val tpe = sym.selfType
      ru.TypeTag(mirror, new api.TypeCreator {
        def apply[U <: api.Universe with Singleton](m: api.Mirror[U]) =
          if (m eq mirror) tpe.asInstanceOf[U#Type]
          else throw new IllegalArgumentException(s"Type tag defined in $mirror cannot be migrated to other mirrors.")
      })
    }
  }

  def javaTypeClass[T](obj: AnyRef, index: Int = 0):Class[T] = JavaReflections.getGenericTypeClass(obj,index).asInstanceOf[Class[T]]
  def javaTypeTag[T](obj: AnyRef, index: Int = 0):ru.TypeTag[T] = typeTag(JavaReflections.getGenericTypeClass(obj,index)).asInstanceOf[ru.TypeTag[T]]
}