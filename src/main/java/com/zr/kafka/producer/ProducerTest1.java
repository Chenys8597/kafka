package com.zr.kafka.producer;

import com.zr.kafka.common.Common;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.Scanner;

/**
 * @Description
 * @Author chenyisheng
 * @Date2019/11/27 14:01
 */
public class ProducerTest1 {

    public static void main(String[] args) {
        Properties props = Common.getProducerProperties();

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), Integer.toString(i)), new Callback() {
                public void onCompletion(RecordMetadata recordMetadata,  Exception e) {
                    if(e == null){
                        System.out.println("secucess");
                    } else {
                        System.out.println("erro");
                    }
                }
            });
        }
    }
}
