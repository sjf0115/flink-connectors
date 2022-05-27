package com.flink.connector.redis.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import redis.clients.jedis.commands.JedisCommands;

/**
 * 功能：RedisStringSinkHandler
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/1 下午7:40
 */
public class RedisStringSinkHandler extends RedisSinkHandler {

    public RedisStringSinkHandler(int keyExpire, Boolean ignoreDelete) {
        super(keyExpire, ignoreDelete);
    }

    @Override
    public void handle(JedisCommands jedis, Tuple2<Boolean, Row> tuple2) {
        Boolean isUpsert = tuple2.f0;
        String key = tuple2.f1.getField(0).toString();
        String value = tuple2.f1.getField(1).toString();
        if(isUpsert) {
            if(this.keyExpire > 0) {
                jedis.setex(key, keyExpire, value);
            } else {
                jedis.set(key, value);
            }
        } else if(!this.ignoreDelete) {
            jedis.del(key);
        }
    }
}
