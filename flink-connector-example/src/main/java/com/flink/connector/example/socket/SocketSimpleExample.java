package com.flink.connector.example.socket;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 功能：Socket Connector 简单示例
 * 作者：SmartSi
 * 博客：http://smartsi.club/
 * 公众号：大数据生态
 * 日期：2022/5/30 下午10:47
 */
public class SocketSimpleExample {
    public static void main(String[] args) throws Exception {
        // TableEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 创建 Socket Source 表
        String sourceSql = "CREATE TABLE socket_source_table (\n" +
                "  word STRING COMMENT '单词',\n" +
                "  frequency BIGINT COMMENT '次数'\n" +
                ") WITH (\n" +
                "  'connector.type' = 'socket',\n" +
                "  'update-mode' = 'append',\n" +
                "  'host' = 'localhost',\n" +
                "  'port' = '9000',\n" +
                "  'delimiter' = '\n',\n" +
                "  'format' = 'json',\n" +
                "  'maxNumRetries' = '3',\n" +
                "  'delayBetweenRetries' = '500'\n" +
                ")";
        tEnv.sqlUpdate(sourceSql);

        Table table = tEnv.sqlQuery("SELECT word, frequency\n" +
                "FROM socket_source_table");
        DataStream dataStream = tEnv.toAppendStream(table, Row.class);
        dataStream.print();

        tEnv.execute("SocketSimpleExample");
    }
}
