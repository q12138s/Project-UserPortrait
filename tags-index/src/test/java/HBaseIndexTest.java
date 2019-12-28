
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class HBaseIndexTest {

    public static void main(String[] args) throws Exception {
        // 1. 获取配置信息
        Configuration conf = HBaseConfiguration.create() ;
        conf.set("hbase.zookeeper.quorum", "bigdata-cdh01.itcast.cn");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        // 2. 获取Connection连接对象
        Connection conn = ConnectionFactory.createConnection(conf) ;

        // 3. 获取Table句柄
        Table table  = conn.getTable(TableName.valueOf("tbl_tags")) ;

        // 4. 创建Put对象，设置值
        List<Put> puts = new ArrayList<Put>();
        Put put = new Put(Bytes.toBytes("102")) ;
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("userId"), Bytes.toBytes("102")) ;
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("tagIds"), Bytes.toBytes("365,370,377")) ;
        puts.add(put);

        Put put2 = new Put(Bytes.toBytes("101")) ;
        put2.addColumn(Bytes.toBytes("user"), Bytes.toBytes("userId"), Bytes.toBytes("101")) ;
        put2.addColumn(Bytes.toBytes("user"), Bytes.toBytes("tagIds"), Bytes.toBytes("363,373,377")) ;
        puts.add(put2);

        Put put3 = new Put(Bytes.toBytes("100")) ;
        put3.addColumn(Bytes.toBytes("user"), Bytes.toBytes("userId"), Bytes.toBytes("100")) ;
        put3.addColumn(Bytes.toBytes("user"), Bytes.toBytes("tagIds"), Bytes.toBytes("366,370,376")) ;
        puts.add(put3);

        // 5. 插入数据至HBase表中
        table.put(puts);

        // 6. 关闭连接
        conn.close();
    }

}