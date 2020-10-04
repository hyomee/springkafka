package com.hyomee.kafkaconsumer;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class KafkaFirstConsumer {

  private Properties conf ;

  public void kafkaSubscriber(String topicName, int cnt) {
    setKafkaProperties();

    // 2. Kafka클러스터에서 Message를 수신(Consume)하는 객체를 생성
    Consumer<Integer, String> consumer = new KafkaConsumer<>(conf);

    // 3. 수신(subscribe)하는 Topic을 등록
    consumer.subscribe(Collections.singletonList(topicName));

    for(int count = 0; count < cnt; count++) {
      // 4. Message를 수신하여, 콘솔에 표시한다
      ConsumerRecords<Integer, String> records = consumer.poll(1);
      for(ConsumerRecord<Integer, String> record: records) {
        String msgString = String.format("key:%d, value:%s", record.key(), record.value());
        System.out.println(msgString);

        // 5. 처리가 완료한 Message의 Offset을 Commit한다
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        OffsetAndMetadata oam = new OffsetAndMetadata(record.offset() + 1);
        Map<TopicPartition, OffsetAndMetadata> commitInfo = Collections.singletonMap(tp, oam);
        consumer.commitSync(commitInfo);
      }

      System.out.println("End ......");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ex ) {
        ex.printStackTrace();
      }
    }

    // 6. KafkaConsumer를 클로즈하여 종료
    consumer.close();


  }

  private void setKafkaProperties() {
    // KafkaConsumer에 필요한 설정
    conf = new Properties();
    conf.setProperty("bootstrap.servers", "localhost:9092");
    conf.setProperty("group.id", "FirstAppConsumerGroup");
    conf.setProperty("enable.auto.commit", "true");
    conf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
    conf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

  }
}
