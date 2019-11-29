package com.zr.kafka.serializer;

import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @Description
 * @Author chenyisheng
 * @Date2019/11/29 11:30
 */
public class UserSerializer implements Serializer {

    private ObjectMapper objectMapper;
    private static Logger logger = LoggerFactory.getLogger(UserSerializer.class);

    @Override
    public void configure(Map configs, boolean isKey) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(String s, Object data) {
        byte[] ret = null;
        try {
            ret = objectMapper.writeValueAsString(data).getBytes("utf-8");
        } catch (Exception e) {
            logger.warn("序列化错误");
        }
        return ret;
    }

    @Override
    public void close() {

    }
}
