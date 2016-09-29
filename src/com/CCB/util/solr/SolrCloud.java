package com.CCB.util.solr;
import java.io.IOException;  
import java.net.MalformedURLException;  
import java.util.ArrayList;  
import java.util.Collection;  
import org.apache.solr.client.solrj.SolrQuery;  
import org.apache.solr.client.solrj.SolrServer;  
import org.apache.solr.client.solrj.SolrServerException;  
import org.apache.solr.client.solrj.impl.CloudSolrServer;  
import org.apache.solr.client.solrj.response.QueryResponse;  
import org.apache.solr.common.SolrDocument;  
import org.apache.solr.common.SolrDocumentList;  
import org.apache.solr.common.SolrInputDocument;  
  
/** 
 * SolrCloud 索引增删查测试 
 * @author ziyuzhang 
 * 
 */  
public class SolrCloud {        
    private static CloudSolrServer cloudSolrServer;    
        
    private  static synchronized CloudSolrServer getCloudSolrServer(final String zkHost) {    
        if(cloudSolrServer == null) {    
            try {    
                cloudSolrServer = new CloudSolrServer(zkHost);    
            }catch(Exception e) {    
                e.printStackTrace();                    
            }    
        }    
            
        return cloudSolrServer;    
    }    
        
  
        
    /**  
     * @param args  
     */    
    public static void main(String[] args) {      
            final String zkHost = "192.168.112.131:2181,192.168.112.132:2181,192.168.112.133:2181";         
            final String  defaultCollection = "nsyh";    
            final int  zkClientTimeout = 20000;    
            final int zkConnectTimeout = 1000;    
                
            CloudSolrServer cloudSolrServer = getCloudSolrServer(zkHost);           
            System.out.println("The Cloud SolrServer Instance has benn created!");              
            cloudSolrServer.setDefaultCollection(defaultCollection);    
            cloudSolrServer.setZkClientTimeout(zkClientTimeout);    
            cloudSolrServer.setZkConnectTimeout(zkConnectTimeout);                     
            cloudSolrServer.connect();    
            System.out.println("The cloud Server has been connected !!!!");              
            //测试实例！    
            SolrCloud test = new SolrCloud();    
            String query = "*:*";
            
            int num = 0;
    		SolrQuery solrQuery = new SolrQuery(query);
    		try {
    			QueryResponse response = cloudSolrServer.query(solrQuery);
    			SolrDocumentList docs = response.getResults();
    			num = (int)docs.getNumFound();
    			System.out.println(num);
    		} catch (IOException | SolrServerException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
    //        System.out.println("测试添加index！！！");         
            //添加index    
    //        test.addIndex(cloudSolrServer);    
                
    //        System.out.println("测试查询query！！！！");    
    //        test.search(cloudSolrServer, "id:*");    
    //            
    //        System.out.println("测试删除！！！！");    
    //        test.deleteAllIndex(cloudSolrServer);    
    //        System.out.println("删除所有文档后的查询结果：");    
//            test.search(cloudSolrServer, "zhan");        
    //        System.out.println("hashCode"+test.hashCode());  
                        
             // release the resource     
            cloudSolrServer.shutdown();    
     
    }    
    
}  