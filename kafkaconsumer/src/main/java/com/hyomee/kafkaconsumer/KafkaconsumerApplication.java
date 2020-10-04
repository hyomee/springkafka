package com.hyomee.kafkaconsumer;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class KafkaconsumerApplication {

  public static void main(String[] args) {
    SpringApplication.run(KafkaconsumerApplication.class, args);
  }


  @Bean
  public ApplicationRunner KafkaProducerRun() {
    return args -> {
      KafkaFirstConsumer KafkaConsumer = new KafkaFirstConsumer();
      KafkaConsumer.kafkaSubscriber("StudyTopic", 5);
    };
  }

}
