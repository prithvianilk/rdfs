package com.rdfs.namenode;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;


public class RedisSingleton {
    private static RedisSingleton redis;
    private RedissonClient redisson;

    private RedisSingleton() {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://127.0.0.1:6379");
        redisson = Redisson.create(config);
    }

    public static RedisSingleton getRedis() {
        if (redis == null) {
            redis = new RedisSingleton();
        }
        return redis;
    }

    public RedissonClient getClient() {
        return redisson;
    }
}
