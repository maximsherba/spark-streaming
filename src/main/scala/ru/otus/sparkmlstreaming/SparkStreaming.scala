package ru.otus.sparkmlstreaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, struct, to_json}
import org.apache.spark.sql.types._

import java.util.Properties

object SparkStreaming {
  def main(args: Array[String]): Unit = {
    // Проверяем аргументы вызова
    if (args.length != 5) {
      System.err.println(
        "Usage: SparkStreaming <path-to-model> <bootstrap-servers> <groupId> <input-topic> <prediction-topic>"
      )
      System.exit(-1)
    }
    val Array(path2model, brokers, groupId, inputTopic, predictionTopic) = args

    // Создаём Streaming Context и получаем Spark Context
    //val sparkConf        = new SparkConf().setAppName("MLStreaming")
    val sparkConf = new SparkConf()
      .setAppName("SparkStreaming")
      .setMaster("local[2]")
    val streamingContext = new StreamingContext(sparkConf, Seconds(1))
    val sparkContext     = streamingContext.sparkContext

    // Параметры подключения к Kafka для чтения
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG        -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG                 -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true"
    )

    // Подписываемся на входную тему Kafka (тема с данными)
    val inputTopicSet = Set(inputTopic)
    val messages = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](inputTopicSet, kafkaParams)
    )

    // Разбиваем входную строку на элементы
    val lines = messages.map(_.value()).map(_.split(","))

    // Загружаем модель
    val model = PipelineModel.load(path2model)
    val outputColumns: Array[String] = Array("sepal_length", "sepal_width", "petal_length", "petal_width", "prediction")

    // Создаём свойства Producer'а для вывода в выходную тему Kafka (тема с расчётом)
    val props: Properties = new Properties()
    props.put("bootstrap.servers", brokers)
    //props.put("topic", predictionTopic)

    // Создаём Kafka Sink (Producer)
    val kafkaSink = sparkContext.broadcast(KafkaSink(props))


    // Обрабатываем каждый входной набор
    lines.foreachRDD { rdd =>
      // Get the singleton instance of SparkSession
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      // Преобразовываем RDD в DataFrame
      val data = rdd
        .toDF("value")
        .withColumn("sepal_length", $"value" (0).cast(DoubleType))
        .withColumn("sepal_width", $"value" (1).cast(DoubleType))
        .withColumn("petal_length", $"value" (2).cast(DoubleType))
        .withColumn("petal_width", $"value" (3).cast(DoubleType))
        .drop("value")

      // Если получили непустой набор данных, передаем входные данные в модель, вычисляем и выводим ID клиента и результат
      if (data.count > 0) {
        val prediction = model.transform(data)
        prediction
          .select(to_json(struct(outputColumns.map(col(_)): _*)).alias("value"))
          .foreach { row => kafkaSink.value.send(predictionTopic, s"${row}") }
          //.foreach {row => println(row)}
      }
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  /** Lazily instantiated singleton instance of SparkSession */
  object SparkSessionSingleton {
    @transient private var instance: SparkSession = _
    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession.builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }
}
