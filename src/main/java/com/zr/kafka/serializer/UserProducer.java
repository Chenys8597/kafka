package com.zr.kafka.serializer;

import com.zr.kafka.common.Common;
import org.apache.kafka.clients.producer.*;

import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

/**
 * @Description
 * @Author chenyisheng
 * @Date2019/11/27 14:01
 */
public class UserProducer {

    public static void main(String[] args) {

        Properties props = Common.getProducerProperties();
        props.put("value.serializer", "com.zr.kafka.serializer.UserSerializer");

        Producer<String, User> producer = new KafkaProducer<String, User>(props);


        Scanner scanner = new Scanner(System.in);
        String nextLine = scanner.nextLine();

        int i = 0;
        while (nextLine != null && !nextLine.equals("")){
            User user = new User(1001l, nextLine, Arrays.asList(new Integer[]{1, 2, 3}));

            producer.send(new ProducerRecord<String, User>("test", Integer.toString(i++), user), new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("发送成功");
                    } else {
                        System.out.println("发送失败");
                    }
                }
            });
            nextLine = scanner.next();
        } ;
    }

}
