package com.adc.hbasedemo;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.util.Properties;

/**
 * kafka+flink+hbase,wordcount处理
 */
public class Kafka2Flink2Hbase {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // checkpoint every 5000 msecs,启用flink检查点之后，flink会定期checkpoint offset，万一作业失败，Flink将把流式程序恢复到最新检查点的状态，并从存储在检查点的偏移量开始重新使用Kafka的记录
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.100.82:9092");
        // properties.put("metadata.broker.list","10.10.10.16:9092");
        //  properties.setProperty("group.id", "test");
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        properties.put("request.required.acks","1");

        FlinkKafkaConsumer<String> myConsumer =  new FlinkKafkaConsumer<String>("testdata", new SimpleStringSchema(), properties);


        myConsumer.setStartFromLatest();

        DataStream<String> stream = env
                .addSource(myConsumer);

        //打印来自Kafka的数据
        stream.print();
        /*DataStream<WordWithCount> wordcount= stream.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String s, Collector<WordWithCount> collector) throws Exception {
                String[] splits = s.split("\\s");
                for (String word:splits){
                    collector.collect(new WordWithCount(word,1L));
                }
            }
        }).keyBy("word").timeWindow(Time.seconds(2))
                .sum("count");

        DataStream<String>  resultString= wordcount.map(new MapFunction<WordWithCount, String>() {
            @Override
            public String map(WordWithCount wordWithCount) throws Exception {
                return wordWithCount.word+"\t"+wordWithCount.count;
            }
        });

        //把数据打印到控制台
        resultString.print()
                .setParallelism(1);//使用一个并行度
*/

        stream.addSink(new HBaseSink2()).setParallelism(1);
       // resultString.process(new HbaseProcess());
      //  resultString.addSink(new SinkToHbase());

        env.execute("Kafka2Flink2Hbase");


    }

    public static class WordWithCount{
        public String word;
        public long count;
        public WordWithCount(){}
        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
