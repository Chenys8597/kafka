package com.zr.kafka.consumer.thread1;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description
 * @Author chenyisheng
 * @Date2019/11/28 18:04
 */
public class ConsumerWorker<K, V> implements Runnable{

    private final ConsumerRecords<K, V> records;
    private final Map<TopicPartition, OffsetAndMetadata> offsets;

    public ConsumerWorker(ConsumerRecords<K, V> records, Map<TopicPartition, OffsetAndMetadata> offsets) {
        this.records = records;
        this.offsets = offsets;
    }

    @Override
    public void run() {
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<K, V>> pRecords = records.records(partition);
            for (ConsumerRecord<K, V> record: pRecords) {
                System.out.println(String.format("topic=%s, partition=%d, offsets=%d", record.topic(), record.partition(), record.offset()));
            }
            long lastOffset = pRecords.get(pRecords.size() - 1).offset();
            synchronized (offsets) {
                if(!offsets.containsKey(partition)) {
                    offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
                } else {
                    long curr = offsets.get(partition).offset();
                    if (curr <= lastOffset + 1) {
                        offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
                    }
                }
            }
        }
    }
}
