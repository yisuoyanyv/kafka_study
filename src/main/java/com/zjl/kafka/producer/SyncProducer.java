package com.zjl.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 同步发送
 */
public class SyncProducer {
    public static void main(String[] args) throws ExecutionException,InterruptedException{

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG,"all");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG,1000);

        // 1.创建一个生产者对象

        KafkaProducer<String, String> producer= new KafkaProducer<String, String>(props);

        // 2.调用send方法
        for (int i = 0; i < 1000; i++) {
            RecordMetadata meta = producer.send(new ProducerRecord<String, String>("first", i + "", "mesage" + i), (metadata, execption) -> {
                if (execption == null) {
                    System.out.println("success");
                } else {
                    execption.printStackTrace();
                }
            }).get();
            System.out.println("partition = " + meta.partition());
        }
        // 3.关闭生产者
        producer.close();

    }
}
