package com.adc.hbasedemo;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.net.URISyntaxException;

public class HbaseSink implements SinkFunction<String> {

    public void invoke(String value,Context context) throws IOException, URISyntaxException {
        Connection connection = null;
        Table table = null;
        try{
        // 加载HBase的配置
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();

            // 读取配置文件
        configuration.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
        configuration.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            configuration.set("hbase.zookeeper.quorum", "192.168.100.80");
            //集群配置↓
            //configuration.set("hbase.zookeeper.quorum", "101.236.39.141,101.236.46.114,101.236.46.113");
          //  configuration.set("hbase.master", "192.168.100.80:60010");
        connection = ConnectionFactory.createConnection(configuration);

        TableName tableName = TableName.valueOf("wordCountTest");
        // 获取表对象
        table = connection.getTable(tableName);

        //hello 2
        String[] split = value.split("\t");
        // 创建一个put请求，用于添加数据或者更新数据

        Put put = new Put(split[0].getBytes());
       // put.addColumn("result".getBytes(), "word".getBytes(), Bytes.toBytes(split[0]));
        put.addColumn("result".getBytes(), "count".getBytes(), split[1].getBytes());
        table.put(put);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } finally {
            if (null != table) table.close();
            if (null != connection) connection.close();
        }

    }


}
