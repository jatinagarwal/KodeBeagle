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

package com.kodebeagle.configuration

import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._
import com.typesafe.config.Config

object KodeBeagleConfig extends ConfigReader{
  def config: Config = ConfigFactory.load()
  var lastIndex=0

  // Output dir for spark job.
   val sparkIndexOutput = get("kodebeagle.spark.index.outputDir").get
   val sparkCheckpointDir = get("kodebeagle.spark.checkpointDir").get
   val sparkRepoOutput = get("kodebeagle.spark.repo.outputDir").get
   val sparkSourceOutput = get("kodebeagle.spark.source.outputDir").get
   val sparkMethodsOutput = get("kodebeagle.spark.method.outputDir").get

   val linesOfContext = get("kodebeagle.indexing.linesOfContext").get
  // This is required to use GithubAPIHelper
   val githubTokens: Array[String] =
    get("kodebeagle.spark.githubTokens").get.split(",")
  // This is required to use GithubAPIHelper
   val githubDir: String = get("kodebeagle.github.crawlDir").get
   val sparkMaster: String = get("kodebeagle.spark.master").get
  
   val esNodes: String = get("kodebeagle.es.nodes").get
   val esPort: String = get("kodebeagle.es.port").get
   val esRepositoryIndex: String = get("kodebeagle.es.repositoryIndex").get
   val esRepoTopicIndex: String = get("kodebeagle.es.repoTopicIndex").get
   val esourceFileIndex: String = get("kodebeagle.es.sourceFileIndex").get
   val eImportsMethodsIndex: String = get("kodebeagle.es.importsmethodsIndex").get

  def nextToken(arr: Array[String] = githubTokens): String = {
    if (lastIndex == arr.length - 1) {
      lastIndex = 0
      arr(lastIndex)
    } else {
      lastIndex = lastIndex + 1
      arr(lastIndex)
    }
  }
}

object TopicModelConfig extends ConfigReader {
  def config: Config = ConfigFactory.load(ConfigFactory.parseResources("topicmodelling.properties"))
   val jobName = get("kodebeagle.ml.topicmodel.jobname").get
   val nbgTopics = get("kodebeagle.ml.topicmodel.nBgTopics").get.toInt
   val nIterations = get("kodebeagle.ml.topicmodel.nIterations").get.toInt
   val nDescriptionWords = 
    get("kodebeagle.ml.topicmodel.nDescriptionWords").get.toInt
   val chkptInterval = get("kodebeagle.ml.topicmodel.chkptInterval").get.toInt
   val batchSize = get("kodebeagle.ml.topicmodel.batchSize").get.toInt
   val save = get("kodebeagle.ml.topicmodel.save").get.trim.toBoolean
   val saveToLocal = get("kodebeagle.ml.topicmodel.saveToLocal").get.trim
}

trait ConfigReader {
  def config: Config
  
  private val settings = scala.collection.mutable.HashMap[String, String]()

  for (c <- config.entrySet())
    yield settings.put(c.getKey, c.getValue.unwrapped.toString)
    
  def get(key: String): Option[String] = settings.get(key)

  def set(key: String, value: String) {
    if (Option(key) == None || Option(value) == None) {
      throw new NullPointerException("key or value can't be null")
    }
    settings.put(key, value)
  }  
}

