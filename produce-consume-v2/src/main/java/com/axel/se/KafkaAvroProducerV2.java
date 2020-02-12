package com.axel.se;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;



public class KafkaAvroProducerV2 {

  public static void main(String[] args) {
    Properties properties = new Properties();

    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
    properties.put(ProducerConfig.ACKS_CONFIG,"1");
    properties.put(ProducerConfig.RETRIES_CONFIG,"10");

    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

    properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,"http://localhost:8081");

    // landoop
    // Gui on http://localhost:3030/

    // using landoop schema registry
    //docker run -it --rm --net=host confluentinc/cp-schema-registry:3.3.1 bash
    //# Then you can do
    //kafka-avro-console-consumer --bootstrap-server 127.0.0.1:9092 --topic customer-avro --property schema.registry.url=http://127.0.0.1:8081
    // the run the producer
    // creates !!! cooool!
    // {"first_name":"Axel","last_name":"Dahlberg","age":31,"height":182.0,"weight":82.0,"automated_email":false}


    KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<>(properties);

    String topic = "customer-avro";

    //breaks with V2 boolean
    //Customer customerV1 = new Customer("Axel","Dahlberg",31,182F,82F,false);

    Customer customer = Customer.newBuilder()
        .setFirstName("Axel")
        .setLastName("Dahlberg")
        .setAge(31)
        .setHeight(182F)
        .setWeight(82F)
        .setPhoneNumber("555-555-555")
        .setEmail("cool@ack.se")
        //.setAutomatedEmail(false) // ooes not exist
        .build();


    //Customer customerV1 = Customer.newBuilder()
    ProducerRecord<String,Customer> producerRecord = new ProducerRecord<>(topic, customer);

    kafkaProducer.send(
        producerRecord,
        (RecordMetadata recordMetadata, Exception e) -> {
          // Exception e = null
          if (Optional.ofNullable(e).isEmpty()) {
            System.out.println("we did it!");
            System.out.println(recordMetadata.toString());
          } else {
            e.printStackTrace();
          }
        });


    kafkaProducer.close();




  }

}



