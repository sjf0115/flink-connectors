package com.flink.connector.example.socket;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
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
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 创建 Socket Source 表
        String sourceSql = "CREATE TABLE socket_source_table (\n" +
                "  word STRING COMMENT '单词',\n" +
                "  frequency STRING COMMENT '频次'\n" +
                ") WITH (\n" +
                "  'connector' = 'socket',\n" +
                "  'hostname' = 'localhost',\n" +
                "  'port' = '9000',\n" +
                "  'byte-delimiter' = '10',\n" +
                "  'format' = 'changelog-csv',\n" +
                "  'changelog-csv.column-delimiter' = '|'\n" +
                ")";
        tEnv.executeSql(sourceSql);

        Table table = tEnv.sqlQuery("SELECT word, frequency\n" +
                "FROM socket_source_table");
        DataStream dataStream = tEnv.toRetractStream(table, Row.class);
        dataStream.print();

        env.execute();
    }
}
