package com.zr.kafka.consumer.thread1;

import com.zr.kafka.common.Common;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Description
 * @Author chenyisheng
 * @Date2019/11/28 17:41
 */
public class ConsumerThreadHandler<K, V> {

    private final KafkaConsumer<K, V> consumer;
    private ExecutorService executorService;
    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    public ConsumerThreadHandler(String brokerList, String groupId, String topic) {
        Properties props = Common.getConsumerProperties(groupId);
        props.put("bootstrap.servers", brokerList);
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                consumer.commitSync(offsets);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                offsets.clear();
            }
        });
    }

    public void consume(int threadNumber) {
        executorService = new ThreadPoolExecutor(threadNumber, threadNumber, 0l, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
        try {
            while (true) {
                ConsumerRecords<K, V> records = consumer.poll(1000l);
                if (!records.isEmpty()){
                    executorService.submit(new ConsumerWorker<>(records, offsets));
                }
            }
        }catch (WakeupException e) {

        } finally {
            commitOffsets();
            consumer.close();
        }
    }

    private void commitOffsets() {
        Map<TopicPartition, OffsetAndMetadata> unmodifiableMap;
        synchronized (offsets) {
            if (offsets.isEmpty()) {
                return;
            }
            unmodifiableMap = Collections.unmodifiableMap(new HashMap<>(offsets));
            offsets.clear();
        }
        consumer.commitSync(unmodifiableMap);
    }

    public void close() {
        consumer.wakeup();
        executorService.shutdown();
    }
}
