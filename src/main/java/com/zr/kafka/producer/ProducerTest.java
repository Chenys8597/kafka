package com.zr.kafka.producer;

import com.zr.kafka.common.Common;
import com.zr.kafka.serializer.User;
import org.apache.kafka.clients.producer.*;

import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

/**
 * @Description
 * @Author chenyisheng
 * @Date2019/11/27 14:01
 */
public class ProducerTest {

    public static void main(String[] args) {
        Properties props = Common.getProducerProperties();

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        Scanner scanner = new Scanner(System.in);
        String nextLine = scanner.nextLine();

        int i = 0;
        while (nextLine != null && !nextLine.equals("")){
            User user = new User(1001l, nextLine, Arrays.asList(new Integer[]{1, 2, 3}));

            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i++), nextLine), new Callback() {
                public void onCompletion(RecordMetadata recordMetadata,  Exception e) {
                    if(e == null){
                        System.out.println("secucess");
                    } else {
                        System.out.println("erro");
                    }
                }
            });
            nextLine = scanner.next();
        } ;
    }
}
