package com.CCB.util.solr;

import java.util.ResourceBundle;

import org.apache.solr.client.solrj.impl.HttpSolrServer;

/**
 * @author Oracle
 *
 */
public class SolrServer {

	private static HttpSolrServer solrServer;
	private static ResourceBundle bundle = ResourceBundle.getBundle("config");
	private static String solrServerConfig = bundle.getString("SolrServer");
	
	//初始化
	static{
//		    System.out.println(solrServerConfig.toString()); 
			solrServer = new HttpSolrServer(solrServerConfig);
			solrServer.setSoTimeout(30000);	//socket read timeout
			solrServer.setConnectionTimeout(30000);
			solrServer.setDefaultMaxConnectionsPerHost(100);
			solrServer.setMaxTotalConnections(200);
			solrServer.setAllowCompression(true);
			solrServer.setMaxRetries(3);
	}
		
	public static HttpSolrServer getSolrServer() {
		return solrServer;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println(solrServer.toString());
		System.out.println(123);
	}

}
