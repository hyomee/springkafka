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

    // 메세지 특정 파티션에 보냄
    for (int i = 0; i < 5; i++) {
      publisher.sendMessageToPartition(message + "Hello To Partitioned Topic! :: " + i, i);
    }

    // 필터 테스트
    publisher.sendMessageToFiltered(message);
    publisher.sendMessageToFiltered("hi " + message);
    publisher.sendMessageToFiltered( message + " hi!");

    // 자바객체 보내기
    publisher.sendMessageToModelVO(message);
  }



}
