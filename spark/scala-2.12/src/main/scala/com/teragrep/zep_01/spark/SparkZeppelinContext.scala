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

package com.teragrep.zep_01.spark

import java.util

import org.apache.spark.SparkContext
import com.teragrep.zep_01.annotation.ZeppelinApi
import com.teragrep.zep_01.display.AngularObjectWatcher
import com.teragrep.zep_01.interpreter.{ZeppelinContext, InterpreterContext, InterpreterHookRegistry}

import scala.collection.Seq
import scala.collection.JavaConverters._


/**
  * ZeppelinContext for Spark
  */
class SparkZeppelinContext(val sc: SparkContext,
                           val sparkShims: SparkShims,
                           val hooks2: InterpreterHookRegistry,
                           val maxResult2: Int) extends ZeppelinContext(hooks2, maxResult2) {

  private val interpreterClassMap = Map(
    "spark" -> "com.teragrep.zep_01.spark.SparkInterpreter",
    "sql" -> "com.teragrep.zep_01.spark.SparkSqlInterpreter",
    "pyspark" -> "com.teragrep.zep_01.spark.PySparkInterpreter"
  )

  private val supportedClasses = scala.collection.mutable.ArrayBuffer[Class[_]]()

  try {
    supportedClasses += Class.forName("org.apache.spark.sql.Dataset")
  } catch {
    case e: ClassNotFoundException =>
  }

  try {
    supportedClasses += Class.forName("org.apache.spark.sql.DataFrame")
  } catch {
    case e: ClassNotFoundException =>

  }
  if (supportedClasses.isEmpty) throw new RuntimeException("Can not load Dataset/DataFrame class")

  override def getSupportedClasses: util.List[Class[_]] = supportedClasses.asJava

  override def getInterpreterClassMap: util.Map[String, String] = interpreterClassMap.asJava

  override def showData(obj: Any, maxResult: Int): String = sparkShims.showDataFrame(obj, maxResult, interpreterContext)

  @ZeppelinApi
  def angularWatch(name: String, func: (AnyRef, AnyRef) => Unit): Unit = {
    angularWatch(name, interpreterContext.getNoteId, func)
  }

  @deprecated
  def angularWatchGlobal(name: String, func: (AnyRef, AnyRef) => Unit): Unit = {
    angularWatch(name, null, func)
  }

  @ZeppelinApi
  def angularWatch(name: String, func: (AnyRef, AnyRef, InterpreterContext) => Unit): Unit = {
    angularWatch(name, interpreterContext.getNoteId, func)
  }

  @deprecated
  def angularWatchGlobal(name: String,
                         func: (AnyRef, AnyRef, InterpreterContext) => Unit): Unit = {
    angularWatch(name, null, func)
  }

  private def angularWatch(name: String, noteId: String, func: (AnyRef, AnyRef) => Unit): Unit = {
    val w = new AngularObjectWatcher(getInterpreterContext) {
      override def watch(oldObject: Any, newObject: AnyRef, context: InterpreterContext): Unit = {
        func(newObject, newObject)
      }
    }
    angularWatch(name, noteId, w)
  }

  private def angularWatch(name: String, noteId: String,
                           func: (AnyRef, AnyRef, InterpreterContext) => Unit): Unit = {
    val w = new AngularObjectWatcher(getInterpreterContext) {
      override def watch(oldObject: AnyRef, newObject: AnyRef, context: InterpreterContext): Unit = {
        func(oldObject, newObject, context)
      }
    }
    angularWatch(name, noteId, w)
  }

  def getAsDataFrame(name: String): Object = {
    sparkShims.getAsDataFrame(get(name).toString)
  }
}
