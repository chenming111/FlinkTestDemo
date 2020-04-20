package com.adc.hbaseUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Hbase增删改查
 */
public class HbaseDemo1 {
    public static  String TABLE_NAME= "testdata";
    public static void main(String[] args) {

        try {

            List<TestDataEntity> list = getAllData(TABLE_NAME);
            System.out.println("Hbase表"+TABLE_NAME+"共有"+list.size()+"条数据");
            for (TestDataEntity testDataEntity : list){
                System.out.println(testDataEntity.toString());
            }
            getNoDealData(TABLE_NAME);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    //连接集群,读取resources下的xml配置文件
    public static Connection initHbase() throws IOException, URISyntaxException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
        configuration.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));
       // configuration.set("hbase.zookeeper.property.clientPort", "2181");
       // configuration.set("hbase.zookeeper.quorum", "10.10.10.16");
        //集群配置↓
        //configuration.set("hbase.zookeeper.quorum", "101.236.39.141,101.236.46.114,101.236.46.113");
      //  configuration.set("hbase.master", "10.10.10.16:60000");
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }

    //创建表
    public static void createTable(String tableNmae, String[] cols) throws IOException, URISyntaxException {

        TableName tableName = TableName.valueOf(tableNmae);
        Admin admin = initHbase().getAdmin();
        if (admin.tableExists(tableName)) {
            System.out.println("表已存在！");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for (String col : cols) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
    }

    //插入数据
    public static void insertData(String tableName, User user) throws IOException, URISyntaxException {
        TableName tablename = TableName.valueOf(tableName);
        Put put = new Put(("user-" + user.getId()).getBytes());
        //参数：1.列族名  2.列名  3.值
        put.addColumn("information".getBytes(), "username".getBytes(), user.getUsername().getBytes()) ;
        put.addColumn("information".getBytes(), "age".getBytes(), user.getAge().getBytes()) ;
        put.addColumn("information".getBytes(), "gender".getBytes(), user.getGender().getBytes()) ;
        put.addColumn("contact".getBytes(), "phone".getBytes(), user.getPhone().getBytes());
        put.addColumn("contact".getBytes(), "email".getBytes(), user.getEmail().getBytes());
        //HTable table = new HTable(initHbase().getConfiguration(),tablename);已弃用
        Table table = initHbase().getTable(tablename);
        table.put(put);
    }

    //获取原始数据
    public static void getNoDealData(String tableName){
        try {
            Table table= initHbase().getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner resutScanner = table.getScanner(scan);
            for(Result result: resutScanner){
                System.out.println("scan:  " + result);
            }
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

    //根据rowKey进行查询
    public static User getDataByRowKey(String tableName, String rowKey) throws IOException, URISyntaxException {

        Table table = initHbase().getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        User user = new User();
        user.setId(rowKey);
        //先判断是否有此条数据
        if(!get.isCheckExistenceOnly()){
            Result result = table.get(get);
            for (Cell cell : result.rawCells()){
                String colName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                if(colName.equals("username")){
                    user.setUsername(value);
                }
                if(colName.equals("age")){
                    user.setAge(value);
                }
                if (colName.equals("gender")){
                    user.setGender(value);
                }
                if (colName.equals("phone")){
                    user.setPhone(value);
                }
                if (colName.equals("email")){
                    user.setEmail(value);
                }
            }
        }
        return user;
    }

    //查询指定单cell内容
    public static String getCellData(String tableName, String rowKey, String family, String col){

        try {
            Table table = initHbase().getTable(TableName.valueOf(tableName));
            String result = null;
            Get get = new Get(rowKey.getBytes());
            if(!get.isCheckExistenceOnly()){
                get.addColumn(Bytes.toBytes(family), Bytes.toBytes(col));
                Result res = table.get(get);
                byte[] resByte = res.getValue(Bytes.toBytes(family), Bytes.toBytes(col));
                return result = Bytes.toString(resByte);
            }else{
                return result = "查询结果不存在";
            }
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
        return "出现异常";
    }

    //查询指定表名中所有的数据
    public static List<TestDataEntity> getAllData(String tableName){

        Table table = null;
        List<TestDataEntity> list = new ArrayList<>();
        try {
            table = initHbase().getTable(TableName.valueOf(tableName));
            ResultScanner results = table.getScanner(new Scan());
            TestDataEntity testDataEntity = null;
            for (Result result : results){
                String id = new String(result.getRow());
               // System.out.println("用户名:" + new String(result.getRow()));
                testDataEntity = new TestDataEntity();
                for(Cell cell : result.rawCells()){
                    String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    //String family =  Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
                    String colName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    testDataEntity.setMdytime(row.split("_")[0]);
                    testDataEntity.setHmstime(row.split("_")[1]);
                    testDataEntity.setMstime(row.split("_")[2]);
                    if(colName.equals("DynoSpeed")){
                        testDataEntity.setDynoSpeed(Double.valueOf(value));

                    }
                    if(colName.equals("DynoTorque")){
                        testDataEntity.setDynoTorque(Double.valueOf(value));
                    }
                    if (colName.equals("IntakeAirPressG")){
                        testDataEntity.setIntakeAirPressG(Double.valueOf(value));
                    }
                    if (colName.equals("CACDP")){
                        testDataEntity.setCACDP(Double.valueOf(value));
                    }
                    if (colName.equals("ExhDownpipePressG")){
                        testDataEntity.setExhDownpipePressG(Double.valueOf(value));
                    }
                    if (colName.equals("ChargeCoolerOutTemp")){
                       testDataEntity.setChargeCoolerOutTemp(Double.valueOf(value));
                    }

                }
                list.add(testDataEntity);

            }
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
        return list;
    }

    //删除指定cell数据
    public static void deleteByRowKey(String tableName, String rowKey) throws IOException, URISyntaxException {

        Table table = initHbase().getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //删除指定列
        //delete.addColumns(Bytes.toBytes("contact"), Bytes.toBytes("email"));
        table.delete(delete);
    }

    //删除表
    public static void deleteTable(String tableName){

        try {
            TableName tablename = TableName.valueOf(tableName);
            Admin admin = initHbase().getAdmin();
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

}
