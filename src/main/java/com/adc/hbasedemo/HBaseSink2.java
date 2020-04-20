package com.adc.hbasedemo;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.util.List;

@Slf4j
public class HBaseSink2 extends RichSinkFunction<String> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Admin admin = HBaseConnPool.getInstance().getConnection().getAdmin();
        //  testdata  为表名,data为列簇
        if (!admin.tableExists(TableName.valueOf("testdata"))) {
            log.info("create hbase table: testdata");
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("testdata"));
            tableDescriptor.addFamily(new HColumnDescriptor("data"));
            admin.createTable(tableDescriptor);
        }
        admin.close();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void invoke(String value, Context context) throws Exception{
        try (Table table = HBaseConnPool.getInstance().getConnection().getTable(TableName.valueOf("testdata"))) {
            String[] split = value.split("\t");
            String rowkey=split[0]+"_"+split[1]+"_"+split[2];
            Put put = new Put(rowkey.getBytes());
             put.addColumn("data".getBytes(), "DynoSpeed".getBytes(), Bytes.toBytes(split[3]));
            put.addColumn("data".getBytes(), "DynoTorque".getBytes(), Bytes.toBytes(split[4]));
            put.addColumn("data".getBytes(), "IntakeAirPressG".getBytes(), Bytes.toBytes(split[5]));
            put.addColumn("data".getBytes(), "CACDP".getBytes(), Bytes.toBytes(split[6]));
            put.addColumn("data".getBytes(), "ExhDownpipePressG".getBytes(), Bytes.toBytes(split[7]));
            put.addColumn("data".getBytes(), "ChargeCoolerOutTemp".getBytes(), Bytes.toBytes(split[8]));

            table.put(put);
        } catch (IOException e) {
            log.error("put error", e);
        }
    }


}


