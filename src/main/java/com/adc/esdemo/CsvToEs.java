package com.adc.esdemo;

import com.adc.hbaseUtil.TestDataEntity;

import com.alibaba.fastjson.JSON;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 读取CSV格式文件，存入ElasticSearch
 */
public class CsvToEs {
    // 设定读取的目录
//        String path = args[0];
    public static String path = "C:\\Users\\chenming\\Desktop\\cold&hot-11\\linshi";
    private static  EsUtil esUtil;
    public static void main(String[] args) {
        esUtil = new EsUtil();
        //读取这个目录下的文件，并发送数据
        while (true){
            try {
                List<File> fileList = new ArrayList<>();
                // addFiles(path, fileList);
                fileList=addFileList(path, fileList);
                for(File file:fileList){
                    System.out.println(file.getName());
                }

                if (fileList.size() != 0)
                    processFileS(fileList);
            }catch (Exception e){
                System.out.println("文件同时读写，但不影响");
            }
        }

    }

    /**
     * 读取文件目录路径下所有以.csv格式结尾的文件，放入List
     * @param path   文件目录路径
     * @param fileList  File格式list
     * @return
     */
    private static List<File> addFileList(String path, List<File> fileList){
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

    /**
     * 输入File格式list，每个File逐个读取文件，存入es，文件处理完毕重命名为xxx.COMPLETE
     * @param fileList
     */

    private static void processFileS(List<File> fileList){
        int id=1;
        for (File file : fileList){
            try {
                LineIterator lineIterator = FileUtils.lineIterator(file, "UTF-8");
                //boolean isHeader = true;
                List<String> columnNames = new ArrayList<>();
                int count =1;
                String filename=file.getName();
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
                      //  sendStringDataToKafka(data,file.getName());
                     //   System.out.println(data2String(data));

                        TestDataEntity testDataEntity = new TestDataEntity(data.split(",")[0],
                                data.split(",")[1],
                                data.split(",")[2],
                                Double.valueOf(data.split(",")[3]),
                                Double.valueOf(data.split(",")[4]),
                                 Double.valueOf(data.split(",")[5]),
                                 Double.valueOf(data.split(",")[6]),
                                Double.valueOf(data.split(",")[7]),
                                Double.valueOf(data.split(",")[8]));

                        esUtil.index("index_"+filename.substring(0, filename.indexOf(".csv")).replace(" ","_"),filename.substring(0, filename.indexOf(".csv")).replace(" ","_"),String.valueOf(id), JSON.toJSONString(testDataEntity));
                        System.out.println(JSON.toJSONString(testDataEntity));
                        id++;
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

}
