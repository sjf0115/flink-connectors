package com.flink.connector.redis.mapper;

public enum RedisCommand {
    LPUSH(RedisDataType.LIST),
    RPUSH(RedisDataType.LIST),
    SADD(RedisDataType.SET),
    SET(RedisDataType.STRING),
    SETEX(RedisDataType.STRING),
    PFADD(RedisDataType.HYPER_LOG_LOG),
    PUBLISH(RedisDataType.PUBSUB),
    ZADD(RedisDataType.SORTED_SET),
    ZINCRBY(RedisDataType.SORTED_SET),
    ZREM(RedisDataType.SORTED_SET),
    HSET(RedisDataType.HASH),
    HINCRBY(RedisDataType.HINCRBY),
    INCRBY(RedisDataType.STRING),
    INCRBY_EX(RedisDataType.STRING),
    DECRBY(RedisDataType.STRING),
    DESCRBY_EX(RedisDataType.STRING);


    private RedisDataType redisDataType;
    RedisCommand(RedisDataType redisDataType) {
        this.redisDataType = redisDataType;
    }
    public RedisDataType getRedisDataType(){
        return redisDataType;
    }
}
