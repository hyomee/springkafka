package com.hyomee.kafka.controller;

import com.hyomee.kafka.producer.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping( value = "/kafka")
public class KafkaController {


  private Publisher publisher;

  @Autowired
  KafkaController(Publisher publisher) {
    this.publisher = publisher;
  }

  @PostMapping(value="/publish")
  public void sendMessageToKafkaTpoic(@RequestParam("message") String message) {
    this.publisher.sendTopicMessage(message);
  }



}
