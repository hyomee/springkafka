package com.hyomee.kafka.consumer;

import com.hyomee.kafka.model.ModelVO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class Subscriber {

  @Value(value="${topic.message.name}")
  private String topicMessageName;

  @Value(value="${topic.modelVo.name}")
  private String topicModelVoName;

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

  @Value(value="${consumerGroup.modelVoGrp}")
  private String modelVoGrp;

  /////////////////////////////////////////////////////
  @KafkaListener(topics = "${topic.message.name}",
                 groupId = "${consumerGroup.hyomeeGrp}",
                 containerFactory = "hyomeeGrpKafkaListenerContainerFactory")
  public void listenHyomeeGrp(String message) {
    log.debug(String.format("#### Topic Test :   Topic : %s, GroupId : %s, Message : %s", topicMessageName , hyomeeGrp, message));
  }

  /////////////////////////////////////////////////////
  @KafkaListener(topics = "${topic.message.name}",
          groupId = "hong",
          containerFactory = "hongKafkaListenerContainerFactory")
  public void listenGroupFoo(String message) {
    log.debug(String.format("#### Topic Test :   Topic : %s, GroupId : %s, Message : %s", topicMessageName , "hong", message));
  }


  //
  @KafkaListener(topics = "${topic.message.name}",
                 containerFactory = "headersKafkaListenerContainerFactory")
  public void listenWithHeaders(@Payload String message,
                                @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
    log.debug(String.format("#### Headers Topic : %s, partition : %s, Message : %s", topicMessageName , partition, message));

  }

  //
  @KafkaListener(topicPartitions = @TopicPartition(topic = "${topic.partitioned.name}",
                                                   partitions = { "0", "3" }),
                                                   containerFactory = "partitionsGrpKafkaListenerContainerFactory")
  public void listenToPartition(@Payload String message,
                                @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
    log.debug(String.format("#### Partition Topic : %s, partition : %s, Message : %s", topicPartitionedName , partition, message));
  }

  //
  @KafkaListener(topics = "${topic.filtered.name}", containerFactory = "filterKafkaListenerContainerFactory")
  public void listenWithFilter(String message) {
    log.debug(String.format("#### Filtered Topic : %s, Message : %s", topicFilteredName , message));

  }

  @KafkaListener(topics = "${topic.modelVo.name}", containerFactory = "modelVoKafkaListenerContainerFactory")
  public void modelVoListener(ModelVO modelVo) {
    log.debug(String.format("#### ModelVo Topic : %s, Message : %s", topicModelVoName , modelVo.toString()));
      }
}
