package com.flink.connector.redis.sink;

import com.flink.connector.redis.mapper.RedisDataType;
import com.flink.connector.redis.options.RedisOption;
import com.flink.connector.redis.options.RedisSinkOption;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;
import redis.clients.jedis.*;
import redis.clients.jedis.commands.JedisCommands;

import javax.annotation.concurrent.GuardedBy;

/**
 * 功能：RedisSinkFunction
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2021/10/1 下午6:56
 */
public class RedisSinkFunction extends RichSinkFunction<Tuple2<Boolean, Row>> {
    private String host;
    private int port;
    private boolean clusterMode;
    private String password;
    private int dataBaseNum;
    private String mode;
    private int keyExpire;
    private boolean ignoreDelete;

    private static volatile JedisCluster jedisCluster;
    @GuardedBy("RedisSinkFunction.class")
    private static volatile JedisPool jedisPool;
    @GuardedBy("RedisSinkFunction.class")
    private static int refCount;

    private RedisSinkHandler handler;
    private RedisDataType redisDataType;

    public RedisSinkFunction(RedisOption redisOption, RedisSinkOption redisSinkOption) {
        this.host = redisOption.getHost();
        this.port = redisOption.getPort();
        this.clusterMode = redisOption.isClusterMode();
        this.password = redisOption.getPassword();
        this.dataBaseNum = redisOption.getDataBaseNum();
        this.mode = redisOption.getMode();
        this.keyExpire = redisSinkOption.getKeyExpire();
        this.ignoreDelete = redisSinkOption.isIgnoreDelete();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if(clusterMode) {
            // 集群模式
            if(StringUtils.isNullOrWhitespaceOnly(password)) {
                jedisCluster = new JedisCluster(new HostAndPort(host, port), 3000, 3000, 50, new JedisPoolConfig());
            } else {
                jedisCluster = new JedisCluster(new HostAndPort(host, port), 3000, 3000, 50, password, new JedisPoolConfig());
            }
        } else {
            // 单机模式
            synchronized (RedisSinkFunction.class) {
                if(jedisPool == null) {
                    String passwd = StringUtils.isNullOrWhitespaceOnly(password) ? null : password;
                    jedisPool = new JedisPool(new JedisPoolConfig(), host, port, 3000, passwd, dataBaseNum);
                }
                refCount ++;
            }
        }

        // SinkHandler
        redisDataType = RedisDataType.valueOf(mode.toUpperCase());
        switch (redisDataType) {
            case STRING:
                handler = new RedisStringSinkHandler(keyExpire, ignoreDelete);
                break;
            default:
                throw new RuntimeException("Not a supported redis mode: " + mode);
        }
    }

    @Override
    public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
        if(clusterMode) {
            // 集群模式
            handler.handle((JedisCommands) jedisCluster, value);
        } else {
            // 单机模式
            Jedis jedis = null;
            try {
                jedis = jedisPool.getResource();
                handler.handle(jedis, value);
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(clusterMode) {
            // 集群模式
            jedisCluster.close();
        } else {
            // 单机模式
            synchronized (RedisSinkFunction.class) {
                refCount --;
                if(refCount <= 0 && jedisPool != null) {
                    jedisPool.close();
                    jedisPool = null;
                }
            }
        }
    }
}
