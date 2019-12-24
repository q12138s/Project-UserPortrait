package cn.itcast.tags.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * 操作HDFS文件系统工具类
 */
object HdfsUtils {

  /**
   * 判断路径是否存在
   * @param conf
   * @return
   */
  def exists(conf:Configuration,path:String): Boolean ={
    //获取文件系统
    val dfs: FileSystem = FileSystem.get(conf)
    //判断路径是否存在
    dfs.exists(new Path(path))
  }

  /**
   * 删除路径
   * @param conf
   * @param path
   */
  def delete(conf:Configuration,path:String): Unit ={
    //获取文件系统
    val dfs: FileSystem = FileSystem.get(conf)
    //如果路径存在就删除
    dfs.deleteOnExit(new Path(path))
  }

}
