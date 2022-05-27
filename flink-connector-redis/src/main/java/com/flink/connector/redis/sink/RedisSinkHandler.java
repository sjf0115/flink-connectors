package com.flink.connector.redis.sink;/**
 * Created by wy on 2021/10/1.
 */

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import redis.clients.jedis.commands.JedisCommands;

/**
 * 功能：
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/1 下午7:47
 */
public abstract class RedisSinkHandler {
    protected int keyExpire;
    protected Boolean ignoreDelete;

    public RedisSinkHandler(int keyExpire, Boolean ignoreDelete) {
        this.keyExpire = keyExpire;
        this.ignoreDelete = ignoreDelete;
    }

    public abstract void handle(JedisCommands jedis, Tuple2<Boolean, Row> tuple2);
}
