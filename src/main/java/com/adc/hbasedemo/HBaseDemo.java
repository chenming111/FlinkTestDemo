package com.adc.hbasedemo;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class HBaseDemo {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        Properties prop = new Properties();
        prop.put("bootstrap.servers", "192.168.100.80:9092");
       // prop.put("group.id", "flink-streaming-job");
        prop.put("serializer.class","kafka.serializer.StringEncoder");
        prop.put("request.required.acks","1");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), prop);
        //source
        DataStream<String> source = env.addSource(consumer);
        DataStream<String> filterDs =  source.filter((FilterFunction<String>) value -> value != null && value.length() > 0);
        filterDs.map((MapFunction<String, List<Put>>) value -> {
                    List<Put> list = new ArrayList<>();
                    String[] args1 = value.split("\t");

                    String rowKey = args1[0] + "_" + args1[1];
                    Put put = new Put(Bytes.toBytes(rowKey));
                    put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("word"), Bytes.toBytes(args1[0]));
                    put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("count"), Bytes.toBytes(args1[1]));
                    list.add(put);
                    return list;
                });
            //    .addSink(new HBaseSink2());

        //HBaseSink中入参是List<Put>，但是上面的实现只传一条数据，明显效率很低
        //用下面的方法批量Sink到HBase
        /*filterDs.countWindowAll(100)
                .apply((AllWindowFunction<String, List<Put>, GlobalWindow>) (window, message, out) -> {
                    List<Put> putList1 = new ArrayList<>();
                    for (String value : message) {
                        String[] columns = value.split(",");
                        String rowKey = columns[0] + "_" + columns[1];
                        Put put = new Put(Bytes.toBytes(rowKey));
                        for (int i = 2; i < columns.length; i++) {
                            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("column" + i), Bytes.toBytes(columns[i]));
                        }
                        putList1.add(put);
                    }
                    out.collect(putList1);
                })
                .addSink(new HBaseSink());*/
       env.execute("HBaseDemo");
    }
}

