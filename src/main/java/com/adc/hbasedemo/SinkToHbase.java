package com.adc.hbasedemo;



import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class SinkToHbase extends RichSinkFunction<String> {

    private static org.apache.hadoop.conf.Configuration configuration;
    private static Connection connection = null;
    private static BufferedMutator mutator;
    private static int count = 0;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        configuration = HBaseConfiguration.create();
      //  configuration.set("hbase.master", "192.168.100.80:60010");
        configuration.set("hbase.zookeeper.quorum", "192.168.100.82");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
      //  configuration.set("zookeeper.znode.parent","/hbase-unsecure");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf("wordCountTest"));
        params.writeBufferSize(2 * 1024 * 1024);
        mutator = connection.getBufferedMutator(params);
        System.out.println("连接成功");
    }

    @Override
    public void close() throws IOException {
        if (mutator != null) {
            mutator.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void invoke(String value, Context context) throws Exception {

        String[] split = value.split("\t");
        //rowkey为当前时间
        String rowkey = String.valueOf(System.currentTimeMillis())+"_"+ split[0];

        Put put = new Put(rowkey.getBytes());

        put.addColumn("result".getBytes(), "word".getBytes(), Bytes.toBytes(split[0]));
        put.addColumn("result".getBytes(), "count".getBytes(), Bytes.toBytes(split[1]));
        mutator.mutate(put);
        //每满500条刷新一下数据
        if (count >= 5){
            mutator.flush();
            count = 0;
        }
        count = count + 1;
        System.out.println("插入成功");
    }


}
