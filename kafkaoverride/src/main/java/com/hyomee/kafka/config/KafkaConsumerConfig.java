package com.hyomee.kafka.config;

import com.hyomee.kafka.model.ModelVO;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

  @Value(value="${kafka.bootstrapAddress}")
  private String bootstrapAddress;

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

  @Value(value="${filter}")
  private String filter;


  public ConsumerFactory<String, String> consumerFactory(String groupId) {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    return new DefaultKafkaConsumerFactory<>(props);
  }


  public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(String groupId) {
    ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory(groupId));
    return factory;
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> hyomeeGrpKafkaListenerContainerFactory() {
    return kafkaListenerContainerFactory(hyomeeGrp);
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> hongKafkaListenerContainerFactory() {
    return kafkaListenerContainerFactory("hong");
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> headersKafkaListenerContainerFactory() {
    return kafkaListenerContainerFactory(headerGrp);
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> partitionsGrpKafkaListenerContainerFactory() {
    return kafkaListenerContainerFactory(partitionsGrp);
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> filterKafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, String> factory = kafkaListenerContainerFactory(filterGrp);
    factory.setRecordFilterStrategy(record -> record.value()
            .contains(filter));
    // factory.setErrorHandler(new SeekToCurrentErrorHandler());
    return factory;
  }


  public ConsumerFactory<String, ModelVO> modelVoConsumerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, modelVoGrp);
    return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new JsonDeserializer<>(ModelVO.class));
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, ModelVO> modelVoKafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, ModelVO> factory = new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(modelVoConsumerFactory());
    return factory;
  }
}
