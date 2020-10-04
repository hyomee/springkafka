package com.hyomee.kafka.contoller;

import com.hyomee.kafka.service.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping( value = "/kafka")
public class KafkaController {

  private Producer producer;

  @Autowired
  KafkaController(Producer producer) {
    this.producer = producer;
  }

  @PostMapping(value="/publish")
  public void sendMessageToKafkaTpoic(@RequestParam("message") String message) {
    this.producer.sendMessage(message);
  }
}
