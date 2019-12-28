import java.io.IOException;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

import java.io.IOException;
/**
 * @author:qisuhai
 * @date:2019/12/28
 * @description:
 */
public class SolrIndexTest {

    String solrUrl = "http://bigdata-cdh01.itcast.cn:9999/solr/tags" ;

    @Test
    public void createIndexToSolr() throws IOException, SolrServerException {
        //a. 创建SolrServer对象
        SolrServer solrServer = new HttpSolrServer(solrUrl);
        //b. 添加document对象
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("user_id", "10001");
        doc.addField("tag_ids", "319,324,329");
        //c. 进行索引的添加
        solrServer.add(doc);
        //d. 提交索引
        solrServer.commit();
    }

    // 3. 查询检索
    @Test
    public void queryIndexToSolr() throws SolrServerException {
        //a. 创建SolrServer对象
        SolrServer solrServer = new HttpSolrServer(solrUrl);

        // b. 执行查询: *:* 查询全部
        //SolrQuery solrQuery = new SolrQuery("*:*");
        SolrQuery solrQuery = new SolrQuery("content:中国人");
        QueryResponse response = solrServer.query(solrQuery);

        //c. 解析response
        SolrDocumentList documents = response.getResults();
        for (SolrDocument document : documents) {
            Object id = document.get("id");
            Object title = document.get("title");
            Object content = document.get("content");
            System.out.println(id + " " + title + " " + content + " ");
        }
    }

    // 2. 删除索引，依据ID删除或者条件删除
    @Test
    public void deleteIndexToSolr() throws IOException, SolrServerException {
        //a. 创建SolrServer对象
        SolrServer solrServer = new HttpSolrServer(solrUrl);
        // b. 执行删除：按照ID删除
        //solrServer.deleteById("10001");
        // 根据条件删除   *:* 删除全部
        solrServer.deleteByQuery("*:*");
        // c. 3.执行提交
        solrServer.commit();
    }


    // 3. 布尔条件查询
    @Test
    public void booleanQueryToSolr() throws IOException, SolrServerException {
        //a. 创建SolrServer对象
        SolrServer solrServer = new HttpSolrServer(solrUrl);
        // b. 执行查询
        /**
         * 1.布尔查询:
         *  AND  OR NOT:
         *  AND : MUST
         *  OR: SHOULD
         *  NOT : MUST_NOT
         */
        SolrQuery solrQuery = new SolrQuery("tag_ids:319 AND tag_ids:324");
        QueryResponse response = solrServer.query(solrQuery);
        SolrDocumentList documents = response.getResults();
        for (SolrDocument document : documents) {
            System.out.println(document);
        }
    }

}
