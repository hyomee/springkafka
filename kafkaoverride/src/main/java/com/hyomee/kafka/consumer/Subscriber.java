package com.hyomee.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class Subscriber {

  @Value(value="${topic.message.name}")
  private String topicMessageName;

  @Value(value="${topic.greeting.name}")
  private String topicGreetingName;

  @Value(value="${topic.filtered.name}")
  private String topicFilteredName;

  @Value(value="${topic.partitioned.name}")
  private String topicPartitionedName;

  /////////////////////////////////////////////////
  @Value(value="${consumerGroup.hyomeeGrp}")
  private String hyomeeGrp;

  @Value(value="${consumerGroup.headerGrp}")
  private String headerGrp;


  @Value(value="${consumerGroup.partitionsGrp}")
  private String partitionsGrp;

  @Value(value="${consumerGroup.filterGrp}")
  private String filterGrp;

  @Value(value="${consumerGroup.greetingGrp}")
  private String greetingGrp;

  /////////////////////////////////////////////////////
  @KafkaListener(topics = "${topic.message.name}", groupId = "${consumerGroup.hyomeeGrp}")
  public void listenGroupFoo(String message) {
    log.info(String.format("Topic : %s, GroupId : %s, Message : %s", topicMessageName , hyomeeGrp, message));
  }
}
