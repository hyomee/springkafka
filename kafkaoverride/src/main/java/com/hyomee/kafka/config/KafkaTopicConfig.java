package com.hyomee.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
public class KafkaTopicConfig {


  @Value(value="${kafka.bootstrapAddress}")
  private String bootstrapAddress;

  @Value(value="${topic.message.name}")
  private String topicMessageName;

  @Value(value="${topic.modelVo.name}")
  private String topicModelVoName;

  @Value(value="${topic.filtered.name}")
  private String topicFilteredName;

  @Value(value="${topic.partitioned.name}")
  private String topicPartitionedName;

  // 새로운 토픽을 등록 하기 위해서 KafkaAdmin 주입
  @Bean
  public KafkaAdmin kafkaAdmin() {
    log.info(String.format("#### Bootstrap Address :: %s", bootstrapAddress));
    Map<String, Object> configs = new HashMap();
    configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    return new KafkaAdmin(configs);
  }

  // 토픽 생성
  @Bean
  public NewTopic topic1() {
    return new NewTopic(topicMessageName, 1, (short) 1);
  }

  @Bean
  public NewTopic topic2() {
    return new NewTopic(topicPartitionedName, 6, (short) 1);
  }

  @Bean
  public NewTopic topic3() {
    return new NewTopic(topicFilteredName, 1, (short) 1);
  }

  @Bean
  public NewTopic topic4() {
    return new NewTopic(topicModelVoName, 1, (short) 1);
  }

}
