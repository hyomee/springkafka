package com.hyomee.kafka;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class KafkaApplication {

  public static void main(String[] args) {
    SpringApplication.run(KafkaApplication.class, args);
  }

  @Bean
  public ApplicationRunner KafkaProducerRun() {
    return args -> {
      KafkaFirstProducer KafkaProducer = new KafkaFirstProducer();
      KafkaProducer.kafkaPublish("StudyTopic", 5);
    };
  }

}
