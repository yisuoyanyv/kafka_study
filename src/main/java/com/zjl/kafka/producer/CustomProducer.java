package com.zjl.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Properties;

public class CustomProducer {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG,"all");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG,1);

        ArrayList<String> interceptors = new ArrayList<>();
        interceptors.add("com.zjl.kafka.interceptor.TimeInterceptor");
        interceptors.add("com.zjl.kafka.interceptor.CounterInterceptor");

        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors);//设置拦截器



        // 1.创建一个生产者对象

        KafkaProducer<String, String> producer= new KafkaProducer<String, String>(props);

        // 2.调用send方法
        for (int i = 0; i < 1000; i++) {
            producer.send(new ProducerRecord<String, String>("first",i+"","mesage"+i));
        }
        // 3.关闭生产者
        producer.close();

    }
}
