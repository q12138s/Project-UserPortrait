package cn.itcast.tags.up;


import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class HDFSUtils {
    // 最好不要使用饿汉式, 因为一开始系统启动就一顿初始化不太好
    private static volatile HDFSUtils instance = null;

    /**
     * 1. 因为可能有多个线程同时访问, 所以在方法上加一把锁
     * 2. 因为在方法上加一把锁, 每次调用的时候大家都很慢, 所以把锁的范围缩小到 创建处
     * 3. 因为把锁的范围缩小了, 所以和没加是一样的, 还是有问题
     * 4. 可以第二次校验, 这样的话, 第一次校验过滤了大部分的调用, 第二次校验完成了单例
     */
    public static HDFSUtils getInstance() {
        if (instance == null) {
            // 只有第一次调用的时候, 才会走这里
            synchronized (HDFSUtils.class) {
                if (instance == null) {
                    instance = new HDFSUtils();
                }
            }
        }
        return instance;
    }

    private Logger logger = LoggerFactory.getLogger(HDFSUtils.class);

    // 因为以下三个东西, 整个类都要用到, 所以放在这
    private Configuration config;
    private URI uri;
    private String user = "root";

    private HDFSUtils() {
        // 创建 Hadoop 配置, 如果 classpath 下存在配置文件, 会自动读取
        config = new Configuration();
        uri = URI.create(config.get("fs.defaultFS"));
    }

    /**
     * 判断文件是否存在
     *
     * 异常存在的目的是好的, 是为了告诉你这个地方有问题, 需要我们去处理这个问题
     */
    public Boolean exists(String path) {
        return handleProcess((fs) -> fs.exists(new Path(path)));
    }

    public Boolean mkdir(String path) {
        return handleProcess((fs) -> fs.mkdirs(new Path(path)));
    }

    public OutputStream createFile(String path) {
        return handleProcess((fs) -> fs.create(new Path(path)));
    }

    /**
     * 移动文件
     * /apps/jars/...jar -> /apps/tags_new/model/...jar
     */
    public Boolean moveFile(String srcPath, String dstPath) {
        return handleProcess((fs) -> fs.rename(new Path(srcPath), new Path(dstPath)));
    }

    public Boolean copyFromInput(InputStream input, String path) {
        return handleProcess((fs) -> {
            FSDataOutputStream output = fs.create(new Path(path));
            IOUtils.copy(input, output);
            return true;
        });
    }

    public Boolean copyFromFile(String src, String dst) {
        return handleProcess((fs) -> {
            fs.copyFromLocalFile(new Path(src), new Path(dst));
            return true;
        });
    }

    /**
     * 问题: 代码冗余
     * 表现:
     *       1. try catch 内写代码, try 是冗余的
     *       2. fs 的创建是冗余的
     *
     * 泛型的使用分为两个部分:
     *       1. 声明泛型, 这个时候, 类型还未被确认
     *       2. 一定要在使用的时候, 确认泛型的类型. 否则, 就是 Object
     */
    private <T> T handleProcess(Processor<T> processor) {
        try {
            FileSystem fs = FileSystem.get(uri, config, user);
            // 真正写代码的地方
            // 能否把代码拿过来, 放在这里执行?
            return processor.process(fs);
        } catch (Exception e) {
            logger.error("访问 HDFS 的时候出现异常", e);
            throw new CommonException();
        }
    }

    interface Processor<T> {

        T process(FileSystem fs) throws Exception;
    }
}
