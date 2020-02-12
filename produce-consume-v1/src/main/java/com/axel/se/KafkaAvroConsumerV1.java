package com.axel.se;

import static java.time.temporal.ChronoUnit.MILLIS;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaAvroConsumerV1 {

  public static boolean running = true;
  public static void main(String[] args) {
    Properties properties = new Properties();

    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");

    properties.put(ConsumerConfig.GROUP_ID_CONFIG,"avro-disiac-consumer-v1");

    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

    properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,"http://localhost:8081");
    properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,"true");

    // version 1
    KafkaConsumer<String,Customer> consumer = new KafkaConsumer<>(properties);

    String topic = "customer-avro";

    consumer.subscribe(Collections.singleton(topic));

    System.out.println("Waiting for data");


    Runtime.getRuntime().addShutdownHook(new Thread(()-> {
      consumer.close();
      System.exit(0);
      System.out.println("Done");
    }));

    while(true) {
      ConsumerRecords<String,Customer> records = consumer.poll(Duration.of(500,MILLIS));
      for(ConsumerRecord<String,Customer> record: records) {
        Customer customer = record.value();
        System.out.println(customer);

      }
      consumer.commitSync();
    }


  }

}
