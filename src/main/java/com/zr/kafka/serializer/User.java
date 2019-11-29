package com.zr.kafka.serializer;


import lombok.Data;

import java.util.List;

/**
 * @Description
 * @Author chenyisheng
 * @Date2019/11/29 11:25
 */
@Data
public class User {
    private Long id;
    private String name;
    private List<Integer> resources;

    public User() {
    }

    public User(Long id, String name, List<Integer> resources) {
        this.id = id;
        this.name = name;
        this.resources = resources;
    }

}
