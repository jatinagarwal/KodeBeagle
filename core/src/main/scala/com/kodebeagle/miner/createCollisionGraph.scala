/*
 *Created by jatina on 22/2/16.
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
package com.kodebeagle.spark

import java.net.InetAddress
import java.util

import com.kb.GraphUtils;
import com.kb.java.model
import com.google.common.collect.Iterables
import com.kb.java.graph.{NamedDirectedGraph, DirectedEdge}
import com.kb.java.model.{Cluster, Clusterer}
;
import com.kb.ml.{KMedoids, DAGClusterMatric}
import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.spark.SparkIndexJobHelper._
import com.kodebeagle.logging.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.eclipse.jdt.core.dom.ASTNode
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.query.QueryBuilders
import org.jgrapht.DirectedGraph
import scala.collection.JavaConversions._
import scala.collection.mutable



object CreateCollisionGraph extends Logger {
  val esPortKey = "es.port"
  val esNodesKey = "es.nodes"
  val jobName = "CreateCollisionGraph"
  val conf = new SparkConf().setAppName(jobName).setMaster("local[*]")
  conf.set("es.http.timeout", "5m")
  conf.set("es.scroll.size", "20")

  val transportClient = new TransportClient(
    ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build()
  ).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.2.67"), 9300))

  def getSourceFileContent(transportClient: TransportClient, fileName: String): String = {
    val fileSourceSearchRequest: SearchRequestBuilder = transportClient.prepareSearch("sourcefile")
      .setTypes("typesourcefile")
      .addField("fileContent");
    val fileSourceSearchResponse: SearchResponse = fileSourceSearchRequest.setQuery(
      QueryBuilders.matchQuery("fileName", fileName)
    ).execute().get()
    fileSourceSearchResponse.getHits.getAt(0).getFields.get("fileContent").getValue[String]
  }

  def main(args: Array[String]) {
    conf.set(esNodesKey, args(0))
    conf.set(esPortKey, args(1))
    val sc: SparkContext = createSparkContext(conf)
    import org.elasticsearch.spark._


    /*Step 1: Finding unique list of all import statements across all the repos */
    val listOfApis = List("java.io.bufferedreader", "java.nio.filechannel", "java.io.printwriter", "java.io.file");

    def query(apiName: String) = "{\n  \"query\": {\n    \"bool\": {\n      \"must\": [\n        {\n          \"term\": {\n            \"typeimportsmethods.tokens.importName\": \""+apiName+"\"\n          }\n        }\n      ],\n      \"must_not\": [],\n      \"should\": []\n    }\n  }\n}"


    /*Step 2: Finding list of fileNames for each statement */

    def getApiNameAndFiles(apiName: String): RDD[((String, Int), String)] = {
      sc.esRDD(KodeBeagleConfig.eImportsMethodsIndex, query(apiName)).map {
        case (repoId, valuesMap) => {
          val fileName: String = valuesMap.get("file").getOrElse("").asInstanceOf[String]
          val score = valuesMap.get("score").getOrElse(null).asInstanceOf[Int]
          (fileName, score) -> (apiName)
        }
      }
    }

    val apiFileInfo: List[RDD[((String, Int), String)]] = listOfApis.map { apiName => getApiNameAndFiles(apiName) }

    val fileWithApiNames: RDD[((String, Int), Iterable[String])] = apiFileInfo.reduceLeft(_ ++ _).groupByKey().filter{
      case((fileName,score),apiName)  => score>=100
    }

    fileWithApiNames.persist()
    val nof: Long = fileWithApiNames.count()
    print("$$$$$$$$$$$$$$$$$$$"+nof+"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
    val a: Int = longRunningOperation();
    print("I slept for 15 seconds"+a)


    val apiGraphs: RDD[(String, List[NamedDirectedGraph])] = fileWithApiNames.flatMap{case(fileInfo,apiNames) =>
      val fileName: String = fileInfo._1
      val fileScore: Int = fileInfo._2
      val fileContent: String = getSourceFileContent(transportClient, fileName)
      val apiMiner: GraphUtils = new GraphUtils()
      val apiWeightedGraphs: List[(String, List[NamedDirectedGraph])] = apiNames.map { api =>
        val graphsInfo: util.HashMap[String, NamedDirectedGraph] = new util.HashMap[String,NamedDirectedGraph]()
        val apiGraphList: util.List[NamedDirectedGraph] = apiMiner.getGraphsFromFile(fileContent,api)
        apiMiner.getNamedDirectedGraphs(graphsInfo,apiGraphList)
        (api,graphsInfo.values().toList)
      }.toList
        apiWeightedGraphs
    }.reduceByKey(_ ++ _)

    apiGraphs.persist()
    val apiGraphsCount: List[(String, Int)] = apiGraphs.mapValues(list => list.size).collect().toList
    apiGraphsCount.foreach { it =>
      val apiName = it._1
      val apiCount = it._2
      println("$$$$$$$$$$$$$" + apiName + "," + apiCount+ "$$$$$$$$$$$$$$$$$$$")
    }
    val noc = 5
    val edgeSupport = 0.2

    val clustering: RDD[(String, List[NamedDirectedGraph])] = apiGraphs.mapValues{it =>
      val clusterClass = new Clusterer()
      val apiMinerMerging = new GraphUtils()
//      it.map{item =>
//        val graphsInstances: util.HashMap[String, NamedDirectedGraph] = pair._1
//        val fileScore = pair._2
      val clusters: util.List[Cluster[NamedDirectedGraph]] = clusterClass.getClusters(it,noc,0.7D)
       clusters.map{cluster =>
        var collisionGraph: NamedDirectedGraph = new NamedDirectedGraph()
        val graphsInCluster = cluster.getInstances
        val mergingGraphsInCluster = graphsInCluster.map{graphInstance =>
          collisionGraph = apiMinerMerging.mergeGraphs(collisionGraph,graphInstance)
        }
        apiMinerMerging.trim(collisionGraph,graphsInCluster.size() * edgeSupport)
        collisionGraph
      }.toList
    }

    println("$$$$$$$$$$$$$"+clustering.count()+"$$$$$$$$$$$$$$$$$$$")

    
    //    import com.kb.java.graph.Node
    //    val apiNameWithGraphs: RDD[(String, Iterable[DirectedGraph[Node, DirectedEdge]])] =
    //      fileWithApiNames flatMap { case ((fileName, score), apiNames) =>
    //      import scala.collection.JavaConversions._
    //      val fileContent = getSourceFileContent(transportClient, fileName)
    //      val cfgResolver = new CFGResolver()
    //      val pars: JavaASTParser = new JavaASTParser(true)
    //      val cu: ASTNode = pars.getAST(fileContent, JavaASTParser.ParseType.COMPILATION_UNIT)
    //      cu.accept(cfgResolver)
    //      val graphs: List[DirectedGraph[Node, DirectedEdge]] = cfgResolver.getMethodCFGs.toList
    //        apiNames.map {apiName => apiName -> filterGraphs(graphs, apiName)}
    //    }

    // clustering

    //    apiNameWithGraphs.map {case (apiName, graphs) => getClusters(graphs, api)}
//    val apiGraphsInfo = apiGraphs.mapValues{pair =>
//      val listOfGraphs = pair.map
//    }
//    import com.google.common.base.Predicate
//    import com.google.common.collect.Iterables

//    def filterGraphs(graphs: List[DirectedGraph[Node, DirectedEdge]], filterString: String):
//    Iterable[DirectedGraph[Node, DirectedEdge]] =
//    {
//      val filteredGraphs: Iterable[DirectedGraph[Node, DirectedEdge]] = Iterables.filter(graphs,
//        new Predicate[DirectedGraph[Node, DirectedEdge]]() {
//        def apply(g: DirectedGraph[Node, DirectedEdge]): Boolean = {
//          import scala.collection.JavaConversions._
//          for (n <- g.vertexSet) {
//            return n.getLabel.contains(filterString)
//          }
//          return false
//        }
//      })
//      return filteredGraphs
//    }

//    def getClusters(instances: util.Collection[NamedDirectedGraph],
//                            n: Int, filterString: String): List[List[NamedDirectedGraph]] =
//    {
//      val dagClusterMatric: DAGClusterMatric = new DAGClusterMatric(filterString, instances.size)
//      val kMedoids: KMedoids[NamedDirectedGraph] = new KMedoids[NamedDirectedGraph](dagClusterMatric, n)
//      val start: Long = System.currentTimeMillis
//      kMedoids.buildClusterer(new util.ArrayList[NamedDirectedGraph](instances))
//      val end: Long = System.currentTimeMillis
//      val total: Int = dagClusterMatric.getHits + dagClusterMatric.getMiss
//      System.out.println("Time taken for clustering : " + instances.size + " graphs was " + (end - start) +
//        "mili secs, Cache hit ratio : " + dagClusterMatric.getHits * 100D / total + ", Cache size: " + dagClusterMatric.getMiss)
//      val clusters: List[List[NamedDirectedGraph]] = kMedoids.getClusters(instances).toList
//      return clusters
//    }



    //    print("############################################"+dd.count()+"################################")
    //    print("######################" + rdd.take(10).toList)
    //    val first1000Files: List[String] = rdd.values.take(1000).toList
    //    val fileNamesBroadcasted = sc.broadcast(first1000Files)
    //
    //    val query1 ="{\"query\":{\"term\":{\"typesourcefile.fileName\":\"PredictionIO/PredictionIO/blob/develop/examples/experimental/java-local-tutorial/src/main/java/recommendations/tutorial4/DataSource.java\"}}}"
    //    val rddFile = sc.esRDD(KodeBeagleConfig.esourceFileIndex, query1).map {
    //      case (repoId, valuesMap) => {
    //        val fileNames: List[String] = fileNamesBroadcasted.value
    //        val fileName= valuesMap.get("fileName").getOrElse("").asInstanceOf[String]
    //        val fileContent = valuesMap.get("fileContent").getOrElse("").asInstanceOf[String]
    //        (fileName,fileContent)
    //      }
    //    }
    //    print("$$$$$$$$$$$$$$$$$"+rddFile.collect().toList)


    /*Step 3: Finding inverted index of file name and corresponding list of api's */


    /*Step 4: Querying and reading file content from sourceFileIndex to obtain CFG's by applying CFG Resolver t*/


    /* Step 5: Flati```````````````````````*/
    sc.stop()
  }

  def longRunningOperation(): Int = { Thread.sleep(15000); 1 }

}
