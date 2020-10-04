package com.hyomee.kafka.producer;

import com.hyomee.kafka.model.ModelVO;
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

  @Autowired
  private KafkaTemplate<String, ModelVO> modelVOKafkaTemplate;

  @Value(value="${topic.message.name}")
  private String topicMessageName;

  @Value(value="${topic.modelVo.name}")
  private String topicModelVoName;

  @Value(value="${topic.filtered.name}")
  private String topicFilteredName;

  @Value(value="${topic.partitioned.name}")
  private String topicPartitionedName;


  public void sendTopicMessage(String message) {
    log.debug(String.format("### Topic Test :: Topic=%s, Message=%s", topicMessageName, message));
    ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicMessageName, message);
    future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
      @Override
      public void onFailure(Throwable throwable) {
        log.error(String.format("### Send message= [ %s ] :: %s "), message, throwable.getMessage());
      }

      @Override
      public void onSuccess(SendResult<String, String> result) {
        log.debug(String.format("### Send message= [ %s ] , Offset  :: %s , "), message, result.getRecordMetadata().offset());
      }
    });
  }


  //
  public void sendMessageToPartition(String message, int partition) {
    log.debug(String.format("### Partition Test : Topic :: %s , Partition :: %s, Message :: %s", topicPartitionedName, partition, message));
    kafkaTemplate.send(topicPartitionedName, partition, null, message);
  }

  //
  public void sendMessageToFiltered(String message) {
    log.debug(String.format("### Filter Test : Topic :: %s , Message :: %s", topicFilteredName, message));
    kafkaTemplate.send(topicFilteredName, message);
  }

  //
  public void sendMessageToModelVO(String message) {
    log.debug(String.format("### ModelVO Test : Topic :: %s , Message :: %s", topicModelVoName, message));
    modelVOKafkaTemplate.send(topicModelVoName, new ModelVO(message, "Hong!"));
  }
}
