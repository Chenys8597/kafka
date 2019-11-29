package com.zr.kafka.serializer;

import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @Description
 * @Author chenyisheng
 * @Date2019/11/29 11:30
 */
public class UserDeserializer implements Deserializer {

    private ObjectMapper objectMapper;
    private static Logger logger = LoggerFactory.getLogger(UserSerializer.class);


    @Override
    public void configure(Map configs, boolean isKey) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public Object deserialize(String s, byte[] data) {
        User user = null;
        try {
            user = objectMapper.readValue(data, User.class);
        }  catch (Exception e) {
            logger.warn("序列化错误");
        } finally {
            return user;
        }
    }

    @Override
    public void close() {

    }
}
