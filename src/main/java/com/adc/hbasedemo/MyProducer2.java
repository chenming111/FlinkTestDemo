package com.adc.hbasedemo;

import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

/**
 * 读取文件夹里csv的数据并发送Kafka ,处理完每个文件，重命名为.COMPLETE
 */
public class MyProducer2 {
    // 设定读取的目录
//        String path = args[0];
    public static String path = "C:\\Users\\chenming\\Desktop\\cold&hot-11\\linshi";
    //Kafka Broker 地址
    public static final String BOOTSTRAP_SERVERS = "192.168.100.82:9092";
    public static KafkaProducer producer;
    public static void main(String[] args) {
        //初始化 Kafka
        initKafka();
        //读取这个目录下的文件，并发送数据
        while (true){
            try {
                List<File> fileList = new ArrayList<>();
               // addFiles(path, fileList);
                fileList=addFileList(path, fileList);

                if (fileList.size() != 0)
                    processFileS(fileList);
            }catch (Exception e){
                System.out.println("文件同时读写，但不影响");
            }
        }

    }

    /**
     * 初始化 Kafka
     */
    private static void initKafka() {
        Properties props = new Properties();
        // kafka 属性设置
        //kafka brokerlist
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        //ack "0,1,-1 all"四种，1为文件patition leader完成写入就算完成
        props.put("acks", "1");
        props.put("retries", 0);
        props.put("batch.size", 409600);
        props.setProperty("group.id", "consumer-group");
        props.put("buffer.memory", 1073741824);
        props.put("linger.ms",15);
        props.put("compression.type","snappy");
        //必须设置(k,v)的序列化  详情见kafkaProducer 的构造函数
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }


    /**
     * Apache Commons IO 实现读取大文本
     * 将 fileList 下的所有文件夹里的数据发送到 Kafka
     * @param fileList 文件集合
     */
    private static void processFileS(List<File> fileList){
        for (File file : fileList){
            try {
                LineIterator lineIterator = FileUtils.lineIterator(file, "UTF-8");
                //boolean isHeader = true;
                List<String> columnNames = new ArrayList<>();
                int count =1;
                while (lineIterator.hasNext()){
                    System.out.println(count);
                    String data = lineIterator.nextLine();
                    if (count<6){
                        count ++;
                        continue;
                    }
                    //第6行为colum列名
                    if (count==6){

                        String[] strings = data.split(",");
                        for (int i = 0; i < strings.length; i++) {
                            if (i==0){
                                strings[i]="Time1";
                            }else if (i==1){
                                strings[i]="Time2";
                            }else if (i==2){
                                strings[i]="Time3";
                            }
                           // strings[i] = strings[i].replace("-","_").replace(" ", "_").toLowerCase().replace("\ufeff","");
                        }
                        Collections.addAll(columnNames, strings);
                        System.out.println(columnNames);
                      //  break;
                       // isHeader = false;
                    }else if (count >=9){
                     //   sendData(data,columnNames,file.getName());
                        sendStringDataToKafka(data,file.getName());
                     //   System.out.println(data2String(data));
                    }
                    count ++;
                }
                //重命名文件前需要关闭操作文件的文件流，才能重命名成功
                lineIterator.close();
                File newFile = new File(file.getPath() + ".COMPLETE");
                boolean flag=file.renameTo(newFile);
                if (flag){
                    System.out.println("重命名正确");
                }else{
                    System.out.println("重命名错误");
                }
            }catch (Exception e){
                System.out.println("有异常");
            }


        }
    }

    /**
     * Guava 实现读取大文本
     * 将 fileList 下的所有文件夹里的数据发送到 Kafka
     * @param fileList 文件集合
     */
    private static void processFileS2(List<File> fileList){
        for (final File file : fileList){
            try {

                Files.asCharSource(file, Charset.defaultCharset()).readLines(new LineProcessor<String>() {
                    boolean isHeader = true;
                    List<String> columnNames = new ArrayList<>();
                    @Override
                    public boolean processLine(String line) throws IOException {
                        if (isHeader){
                            String[] strings = line.split(",");
                            for (int i = 0; i < strings.length; i++) {
                                strings[i] = strings[i].replace("-","_").replace(" ", "_").toLowerCase().replace("\ufeff","");
                            }
                            Collections.addAll(columnNames, strings);
                            isHeader = false;
                        }else{
                            sendData(line,columnNames,file.getName());
                        }
                        return true;
                    }

                    @Override
                    public String getResult() {
                        return null;
                    }
                });
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }


    /**
     * BufferedReader 实现读取大文本
     * 将 fileList 下的所有文件夹里的数据发送到 Kafka
     * @param fileList 文件集合
     */
    private static void processFile(List<File> fileList) {
        for (File file : fileList){
            FileInputStream fis = null;
            InputStreamReader isr;
            BufferedReader br;
            try {
                fis = new FileInputStream(file);
                isr = new InputStreamReader(fis);
                br = new BufferedReader(isr);
                String data;
                boolean isHeader = true;
                List<String> columnNames = new ArrayList<>();
                while ((data = br.readLine()) != null) {
                    if (isHeader){//读取表头
                        String[] strings = data.split(",");
                        for (int i = 0; i < strings.length; i++) {
                            strings[i] = strings[i].replace("-","_").replace(" ", "_").toLowerCase().replace("\ufeff","");
                        }
                        Collections.addAll(columnNames, strings);
                        isHeader = false;
                    }else {
                       sendData(data,columnNames,file.getName());//发送数据
                    }
                }
                fis.close();
                isr.close();
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            //一个文件里的数据发送完之后，将文件重命名为以 .COMPLETE 结尾的文件名，以避免重复读
            file.renameTo(new File(file.getPath() + ".COMPLETE"));
        }
    }

    /**
     *
     * @param data
     * @param filename
     */
    private static void sendStringDataToKafka(String data,String filename){
        producer.send(new ProducerRecord<>(filename.substring(0, filename.indexOf(".csv")).replace(" ","_"),data2String(data)));
        System.out.println(data2String(data));
    }
    /**
     * 发送数据
     * @param data 数据
     * @param columnNames 列名集合
     * @param filename 文件名
     */
    private static void sendData(String data, List<String> columnNames, String filename) {

        //将一行数据转换成 json 格式，并发送给 Kafka
        // 将数据发送到 filename 去掉 .csv 并用 _ 替换 @ 后的 topic 中
        // 将为 NULL 的数据进行特殊处理，替换成 -1111111 ，以便过滤
        producer.send(
                new ProducerRecord<>(filename.substring(0, filename.indexOf(".csv")).replace(" ","_"), data2Json(data, columnNames, filename).replace("NULL","-1111111")));
        System.out.println(data2Json(data, columnNames, filename));
    }

    /**
     *
     * @param data
     * @return
     */
    private static String data2String(String data){
        StringBuffer sb = new StringBuffer();
        String[] values = data.split(",");
        //空值用负数-1111111填充
        for (int i=0;i<values.length;i++){
            if ("".equals(values[i])){
                values[i]="-1111111";
            }
            sb.append(values[i]).append("\t");
        }
        return sb.toString();
    }

    /**
     * 将标注表和传感器数据分开处理
     * 标注表，将 vin、session、startTime、endTime、p_startTime、p_endTime 其他的列全部用 | 分隔并合并到一列中
     * @param data 一行以逗号分割列的数据
     * @param columnNames 列名
     * @return json
     */
    private static String data2Json(String data, List<String> columnNames, String fileName) {
        if ("ScenarioTable.csv".equals(fileName)){//处理传感器数据
            StringBuilder primaryBuilder = new StringBuilder("{");
            StringBuilder tagBuilder = new StringBuilder("\"tag\":\"");
            String[] values = data.split(",");
            int i = 0;
            for (String name :
                    columnNames) {
                //标注表，将 vin、session、startTime、endTime、p_startTime、p_endTime 其他的列全部用 | 分隔并合并到 tag 列中
                // startTime 8 endTime 9 p_startTime 13 p_endTime 14 vin 19 session 20
                boolean flag = i == 8 || i ==9 || i == 13 || i == 14 || i == 19 || i == 20;
                if (flag){
                    primaryBuilder.append("\"").append(name).append("\":").append("\"").append(values[i ++]).append("\"").append(",");
                }else {
                    tagBuilder.append(name).append("=").append(values[i ++]).append("|");
                }
            }
            tagBuilder.deleteCharAt(tagBuilder.lastIndexOf("|")).append("\"}");
            primaryBuilder.append(tagBuilder);
            return primaryBuilder.toString();
        }else {
            StringBuilder builder = new StringBuilder("{");
            String[] values = data.split(",");
            int i = 0;
//        builder.append("\"fileName\":").append("\"").append(fileName).append("\"").append(",");
            for (String name:
                    columnNames) {
                if ("time".equals(name)) {//time 在 Flink TableSQL 中算关键字，所以需要替换
                    builder.append("\"").append("timeRecord").append("\":").append("\"").append(values[i ++]).append("\"").append(",");
                }else {
                    builder.append("\"").append(name).append("\":").append("\"").append(values[i ++]).append("\"").append(",");
                }
            }
            //TODO test 删除
            builder.append("\"uuid\":\"").append(UUID.randomUUID()).append("\"").append(",");
            builder.deleteCharAt(builder.lastIndexOf(",")).append("}");
            return builder.toString();
        }
    }

    /**
     * 递归地将文件夹下的所有非以 COMPLETE 结尾的文件加入到 fileList 集合中
     * @param path 读取的文件夹路径
     * @param fileList file 集合
     */
    private static void addFiles(String path, List<File> fileList) {
        File parentDirectory = new File(path);
        File[] files = parentDirectory.listFiles();
        if (files == null){
            return;
        }
        for (File file : files){
            String filename = file.getName();
            //读取非以 COMPLETE 结尾的文件
            if (file.isFile() && "csv".equals(filename.substring(filename.lastIndexOf(".") + 1)) &&!"COMPLETE".equals(filename.substring(filename.lastIndexOf(".") + 1))){
                fileList.add(file);
            }else {
                addFiles(file.getPath(),fileList);
            }
        }
    }


    private static List<File> addFileList(String path,List<File> fileList){
        File parentDirectory = new File(path);
        File[] files = parentDirectory.listFiles();
        if (files == null){
            return null;
        }
        for (File file : files){
            String filename = file.getName();
            //读取非以 COMPLETE 结尾的文件
            //&&!"COMPLETE".equals(filename.substring(filename.lastIndexOf(".") + 1))
            if (file.isFile() && "csv".equals(filename.substring(filename.lastIndexOf(".") + 1)) ){
                fileList.add(file);
            }else {
                continue;

               // addFiles(file.getPath(),fileList);

            }
        }

        return fileList;

    }
}
