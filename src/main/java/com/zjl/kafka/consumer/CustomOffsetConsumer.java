package com.zjl.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

public class CustomOffsetConsumer {
    public static void main(String[] args) {
        Properties props=new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);// 自动提交， 为false时每次消费都会从上次的offset开始消费
        //props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");//提交offset时间间隔 自动提交时设置
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"12");
        // 1.创建一个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("first"), new ConsumerRebalanceListener() {
            //提交当前负责的分区的offset
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("==================回收的分区==============");
                for (TopicPartition partition : partitions) {
                    System.out.println("partition = " + partition);
                }
            }

            //定位新分配的分区的offset
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

                System.out.println("==================重新得到的分区==============");
                for (TopicPartition partition : partitions) {
                  Long offset=getPartitionOffset(partition);
                  consumer.seek(partition,offset);
                }
            }
        });
        // 2.调用poll
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d,key = %s,value = %s%n "+record.offset(),record.key(),record.value());
                TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                commitOffset(topicPartition,record.offset()+1);
            }

        }


    }

    private static void commitOffset(TopicPartition topicPartition, long l) {
        //TODO
    }

    private static Long getPartitionOffset(TopicPartition partition) {
        //TODO
        return 100L;
    }
}
