package com.hyomee.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class KafkaFirstProducer {

  private Properties conf ;

  public void kafkaPublish(String topicName, int loopcnt) {

    // kafka 환셩 설정
    setKafkaProperties();

    // Kafka Cluster에 메세지를 송신하는 객체 생성
    Producer<Integer, String> producer = new KafkaProducer<>(this.conf);
    System.out.println("Kafka Cluster에 메세지를 송신하는 객체 생성");
    int key;
    String value;
    for ( int i = 1; i <= loopcnt; i++) {
      key = i;
      value = "메세지 :: " + String.valueOf(i);

      System.out.println("Kafka Cluster :: " + value);
      //송신할 메세지 생성
      ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName, key, value);

      //메세지 송신
      producer.send(record, new Callback() {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
          if (recordMetadata != null) {
            // 송신에 성공한 경우의 처리
            String infoString = String.format("Success partition:%d, offset:%d",
                    recordMetadata.partition(),
                    recordMetadata.offset());
            System.out.println(infoString);
          } else {
            // 송신에 실패한 경우의 처리
            String infoString = String.format("Failed:%s", e.getMessage());
            System.err.println(infoString);
          }
        }
      });
    }

    producer.close();
  }


  private void setKafkaProperties() {
    // Kafka Producer에 필요한 설정
    this.conf = new Properties();
    // Kafka Producer가 접속 하는 브로커의 호스명과 포트
    // 예) kafka-broker01:9092, kafka-broker02:9092, kafka-broker03:9092
    this.conf.setProperty("bootstrap.servers", "localhost:9092");

    // kafka의 모든 메시지는 직렬하된 상태로 전송하므로 직렬화 처리에 이용되는 시리얼라이저 클래스 지정로
    // key와 Value를 각각 설정 함
    this.conf.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer" );
    conf.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  }

}
