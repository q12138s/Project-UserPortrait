package cn.itcast.tags.index.hbase;

import cn.itcast.tags.index.tools.SolrTools;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.List;

/**
 * 当新增、删除用户持有的标签时，会同步索引到solr中
 */
public class HBaseSolrIndexCoprocesser extends BaseRegionObserver {

    /**
     * Put操作后同步索引到Solr中
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e,
                        Put put, WALEdit edit, Durability durability) throws IOException {
        // 1. 构建Doc数据：SolrInputDocument
        SolrInputDocument doc = new SolrInputDocument() ;
        // 2. 依据列簇和列名称获取值
        for(String column: SolrTools.HBASE_TABLE_COLUMNS){
            // 由于HBase存储数据是多版本的
            List<Cell> cells = put.get(
                    Bytes.toBytes(SolrTools.HBASE_TABLE_FAMILY), Bytes.toBytes(column)
            ) ;
            // 判断获取Cells是否有值
            if(null != cells && cells.size() > 0){
                // 获取第一个版本的值
                Cell cell = cells.get(0) ;
                // 从Cell中获取值, 转换为String类型
                String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                // 将字段设置到SolrInputDocument中
                if("userId".equals(column)){
                    doc.addField("user_id", columnValue);
                }else if("tagIds".equals(column)){
                    doc.addField("tag_ids", columnValue);
                }
            }
        }
        // 3. 将SolrInputDocument插入到Solr索引库中
        SolrTools.addDoc(doc);
    }

    /**
     * Delete操作后清除Solr中的索引
     */
    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e,
                           Delete delete, WALEdit edit, Durability durability) throws IOException {
        // 1. 获取用户ID
        String userId = Bytes.toString( delete.getRow()) ;
        // 2. 依据useId将Solr索引库中数据删除
        SolrTools.deteleDoc(userId);
    }
}
