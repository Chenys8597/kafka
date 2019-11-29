package com.zr.kafka.serializer;

import com.zr.kafka.common.Common;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @Description
 * @Author chenyisheng
 * @Date2019/11/28 10:43
 */
public class UserConsumer {

    public static void main(String[] args) {

        List<String> topics = new ArrayList<String>();
        topics.add("test");

        KafkaConsumer<String, User> consumer = getOneConsumer(topics, "test-group2");

        try {
            while (true) {
                ConsumerRecords<String, User> records = consumer.poll(1000);
                for (ConsumerRecord<String, User> record : records){
                    User user = record.value();
                    System.out.printf("consumer1: offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }

    public static  KafkaConsumer<String, User> getOneConsumer(List<String> topics, String groupID){
        Properties props = Common.getConsumerProperties(groupID);
        props.put("value.deserializer", "com.zr.kafka.serializer.UserDeserializer");
        KafkaConsumer<String, User> consumer = new KafkaConsumer<String, User>(props);
        consumer.subscribe(topics);
        return consumer;
    }
}
