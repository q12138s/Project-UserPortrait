package cn.itcast.tags.index.tools;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

/**
 * 对Solr中索引进行操作工具类：插入索引和删除索引
 */
public class SolrTools {

    // 加载属性配置文件
    private static Config config = ConfigFactory.load("solr.properties");
    // 获取Solr Url地址
    private static final String SOLR_URL = config.getString("solr.addr");
    // 获取用户标签表列簇和列名称
    public static final String HBASE_TABLE_FAMILY = config.getString("solr.profile.family");
    public static final String[] HBASE_TABLE_COLUMNS = config.getString("solr.profile.fields").split(",");

    /**
     * 向Solr中插入索引
     *
     * @param doc 文档
     */
    public static void addDoc(SolrInputDocument doc) {
        // 1. 将SolrInputDocument插入到Solr索引库中
        SolrServer solrServer = null;
        try {
            solrServer = new HttpSolrServer(SOLR_URL);
            // 插入索引
            solrServer.add(doc);
            // 提交
            solrServer.commit();
        } catch (Exception exception) {
            exception.printStackTrace();
        } finally {
            // 关闭
            if (null != solrServer) {
                solrServer.shutdown();
            }
        }
    }

    /**
     * 依据Solr索引中Id删除
     *
     * @param id 索引ID
     */
    public static void deteleDoc(String id) {
        SolrServer solrServer = null;
        try {
            solrServer = new HttpSolrServer(SOLR_URL);
            // 删除索引
            solrServer.deleteById(id);
            // 提交执行
            solrServer.commit();
        } catch (Exception exception) {
            exception.printStackTrace();
        } finally {
            // 关闭资源
            if (null != solrServer) {
                solrServer.shutdown();
            }
        }
    }
}
