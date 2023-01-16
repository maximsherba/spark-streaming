# Spark Streaming + Kafka 

Пример простого приложения Spark Streaming, которое читает данные из Kafka, прогоняет их через ML модель и загружает в другой топик Kafka

## Запуск

* Запускаем Kafka в докере - docker-compose.yml
* Запускаем загрузку данных во входящий топик
sudo awk -F ',' 'NR > 1 { system("sleep 1"); print $1 "," $2 "," $3 "," $4 }' < spark/data/IRIS.csv | sudo docker exec -i kafka-broker kafka-console-producer --topic input --bootstrap-server kafka:9092
* Запускаем spark-submit с параметрами 
spark-submit /spark/Scala/SparkStreaming-assembly-1.0.jar /spark/Scala/IrisModel localhost:29092 homework input1 prediction1