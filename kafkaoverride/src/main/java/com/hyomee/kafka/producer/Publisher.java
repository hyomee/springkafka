package com.hyomee.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Slf4j
@Service
public class Publisher {

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  @Value(value="${topic.message.name}")
  private String topicMessageName;

  @Value(value="${topic.greeting.name}")
  private String topicGreetingName;

  @Value(value="${topic.filtered.name}")
  private String topicFilteredName;

  @Value(value="${topic.partitioned.name}")
  private String topicPartitionedName;


  public void sendTopicMessage(String message) {
    log.info(String.format("Topic=%s, Message=%s", topicMessageName, message));
    ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicMessageName, message);
    future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
      @Override
      public void onFailure(Throwable throwable) {
        log.error(String.format("Send message= [ %s ] :: %s "), message, throwable.getMessage());
      }

      @Override
      public void onSuccess(SendResult<String, String> result) {
        log.info(String.format("Send message= [ %s ] , Offset  :: %s , "), message, result.getRecordMetadata().offset());
      }
    });
  }
}
