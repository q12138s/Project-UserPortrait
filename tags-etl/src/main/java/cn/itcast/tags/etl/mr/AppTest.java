package cn.itcast.tags.etl.mr;

import java.io.File;

/**
 * @author:qisuhai
 * @date:2019/12/16
 * @description:
 */
public class AppTest {


        public static void main(String[] args) {
            File file = new File("E:\\bigdata_project\\profile_day07[20191224]\\02_视频");
            if(file.isDirectory()){
                File[] files = file.listFiles();
                for (File file1 : files) {
                    System.out.println(file1.getName().substring(0,file1.getName().lastIndexOf(".")));
                }
            }
        }

    }

