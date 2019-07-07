package com.github.tiger.elastic

import java.net.InetAddress
import java.util.concurrent.TimeUnit

import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.{InetSocketTransportAddress, TransportAddress}
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import org.elasticsearch.index.reindex.DeleteByQueryAction
import org.elasticsearch.transport.client.PreBuiltTransportClient

/**
  * 传输客户端允许创建不属于群集的客户端，但只需通过使用添加其各自的地址直接连接到一个或多个节点。
  *
  * 执行索引、文档的增删改查操作。
  */
class BasisCurdTransportClient(settings: Settings, transportAddresses: Array[TransportAddress])
  extends AutoCloseable {

  private[elastic] val _transportClient = TransportClientSingleton
    .instance(settings, transportAddresses)

  //----- 索引 -----

  /**
    * 是否存在索引
    */
  def isIndexExist(_index: String): Boolean = {
    _transportClient.admin().indices()
      .prepareExists(_index).get().isExists()
  }

  /**
    * 创建索引
    */
  def createIndex(_index: String): Boolean = {
    if (isIndexExist(_index)) {
      false
    }
    else {
      _transportClient.admin().indices()
        .prepareCreate(_index).get().isAcknowledged
    }
  }

  /**
    * 是否存在类型
    */
  def isTypeExist(_index: String, _type: String): Boolean = {
    if (!isIndexExist(_index)) {
      false
    } else {
      _transportClient.admin.indices()
        .prepareTypesExists(_index).setTypes(_type).get().isExists
    }
  }

  /**
    * 创建指定映射的索引/类型
    */
  def createIndexAndTypeWithMapping(_index: String, _type: String,
                                    _mapping: XContentBuilder): Boolean = {
    if (isTypeExist(_index, _type)) {
      false
    } else {
      _transportClient.admin.indices().prepareCreate(_index)
        .addMapping(_type, _mapping).get().isAcknowledged()
    }
  }

  /**
    * 删除索引
    */
  def deleteIndex(_index: String): Boolean = {
    if (!isIndexExist(_index)) {
      false
    }
    else {
      _transportClient.admin().indices()
        .prepareDelete(_index).get().isAcknowledged()
    }
  }

  //----- 文档 -----

  /**
    * 根据 索引/类型，添加文档
    */
  def saveDocument(_index: String, _type: String, _id: String,
                   _source: XContentBuilder): Long = {
    val response = _transportClient.prepareIndex(_index, _type, _id)
      .setSource(_source).get()
    response.getVersion
  }

  /**
    * 根据 索引/类型/ID，获取文档
    */
  def getDocumentById(_index: String, _type: String, _id: String): String = {
    val response = _transportClient.prepareGet(_index, _type, _id).get()
    val _source = response.getSourceAsString
    _source
  }

  /**
    * 根据 索引/类型/ID，删除文档
    *
    * deleteOneById("spark", "docs", "AWUO5mkeTX6oBJMPTiEl")
    */
  def deleteDocumentOneById(_index: String, _type: String, _id: String): Unit = {
    _transportClient.prepareDelete(_index, _type, _id).get()
  }

  /**
    * 根据查询结果删除文档
    *
    * For match query:
    *
    * deleteByQuery(Array("spark", "spark-streaming"),
    *     QueryBuilders.matchQuery("id", "1"))
    *
    * For query string query:
    *
    * val queryString = "keyword:*"
    * deleteByQuery(Array("spark", "spark-streaming"),
    *     QueryBuilders.queryStringQuery(queryString))
    */
  def deleteDocumentByQuery(_index: Array[String], _query: QueryBuilder): Unit = {
    DeleteByQueryAction.INSTANCE
      .newRequestBuilder(_transportClient).filter(_query)
      .source(_index: _*).get()
  }

  override def close(): Unit = _transportClient.close()
}

object TransportClientSingleton {

  var _instance: TransportClient = null

  def instance(settings: Settings,
               transportAddresses: Array[TransportAddress]): TransportClient = {
    if (_instance == null) {
      _instance = new PreBuiltTransportClient(settings)
        .addTransportAddresses(transportAddresses: _*)
    }

    _instance
  }
}

object BasisCurdTransportClient {

  def apply(settings: Settings,
            transportAddresses: Array[TransportAddress]): BasisCurdTransportClient =
    new BasisCurdTransportClient(settings, transportAddresses)

  def main(args: Array[String]): Unit = {

    val settings = Settings.builder()
      // 集群名称
      .put("cluster.name", "campus-logs")
      // 自动嗅探
      .put("client.transport.sniff", true)
      .build();

    // 集群地址
    val transportAddresses = Array(
      new InetSocketTransportAddress(
        InetAddress.getByName("175.63.101.107"), 9300).asInstanceOf[TransportAddress]
    )


    val queryString = "keyword:*"
    (0 until 10000).foreach(_ => {
      val client = BasisCurdTransportClient(settings, transportAddresses)
      client.deleteDocumentByQuery(Array("spark-streaming"),
        QueryBuilders.queryStringQuery(queryString))

      TimeUnit.MILLISECONDS.sleep(20)
    })


    //    val index = "spark-streaming"
    //    val `type` = "kw-total"
    //
    //        val mapping = XContentFactory.jsonBuilder()
    //          .startObject().startObject("properties")
    //          .startObject("id").field("type", "string").endObject()
    //          .startObject("keyword").field("type", "string").endObject()
    //          .startObject("count").field("type", "long").endObject()
    //          .startObject("createDate").field("type", "date").field("format", "strict_date_optional_time||epoch_millis").endObject()
    //          .endObject().endObject();
    //
    //        println(client.createIndexAndTypeWithMapping(index, `type`, mapping))

    //    val document = XContentFactory.jsonBuilder()
    //      .startObject()
    //      .field("keyword", "Golang")
    //      .field("count", 10)
    //      .field("createDate", "2018-09-20T00:00:00+0800")
    //      .endObject();
    //
    //    val id = ""
    //    println(client.saveDocument(index, `type`, id, document))
  }

}
