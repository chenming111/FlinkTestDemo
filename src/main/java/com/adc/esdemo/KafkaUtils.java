package com.adc.esdemo;

import com.alibaba.fastjson.JSON;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
/**
 * Kafka生产数据
 */
public class KafkaUtils {
    private static final String broker_list = "hadoop000:9092";
    private static final String topic = "student-1";  //kafka topic 需要和 flink 程序用同一个 topic

    public static void writeToKafka() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        KafkaProducer producer = new KafkaProducer<String, String>(props);//老版本producer已废弃
        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);

        try {
            for (int i = 1; i <= 100; i++) {
                Student student = new Student(i, "itzzy" + i, "password" + i, 18 + i);
                ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(student));
                producer.send(record);
                System.out.println("发送数据: " + JSON.toJSONString(student));
            }
            Thread.sleep(3000);
        }catch (Exception e){

        }

        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        writeToKafka();
    }
}
