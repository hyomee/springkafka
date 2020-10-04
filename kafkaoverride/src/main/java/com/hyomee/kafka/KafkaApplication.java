package com.hyomee.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaApplication {

  // https://github.com/eugenp/tutorials/blob/master/spring-kafka/src/main/java/com/baeldung/spring/kafka/KafkaApplication.java
  public static void main(String[] args) {
    SpringApplication.run(KafkaApplication.class, args);
  }

}
