package com.zr.kafka.consumer.thread1;

/**
 * @Description
 * @Author chenyisheng
 * @Date2019/11/29 9:50
 */
public class Main {
    public static void main(String[] args) {

        String brokerList = "localhost:9092";
        String groupId = "test-group1";
        String topic = "test";

        final ConsumerThreadHandler<String , String> handler = new ConsumerThreadHandler<>(brokerList, groupId, topic);
        final int cpuCount = Runtime.getRuntime().availableProcessors();
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                handler.consume(cpuCount);
            }
        };

        new Thread(runnable).start();

        try {
            Thread.sleep(20000l);
        } catch (InterruptedException e) {

        }
//        System.out.println("Starting to close the consumer...");
//        handler.close();
    }
}
