package com.CCB.util.solr;

import java.io.IOException;
import java.util.ResourceBundle;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.cloud.ZkStateReader;

public class SolrCloudServer {

	private static ResourceBundle bundle = ResourceBundle.getBundle("config");
	private static String solrServerConfig = bundle.getString("SolrServer");
	private static String zkHost = bundle.getString("zkHost");
	private static int zkClientTimeout = 20000;
	private static int zkConnectTimeout = 1000;
	private static CloudSolrServer cloudSolrServer;
	private static String defaultCollection = bundle
			.getString("defaultCollection");

	public static synchronized CloudSolrServer getCloudSolrServer() {

		if (cloudSolrServer == null) {
			cloudSolrServer = new CloudSolrServer(zkHost);
			cloudSolrServer.setZkClientTimeout(zkClientTimeout);
			cloudSolrServer.setZkConnectTimeout(zkConnectTimeout);
			cloudSolrServer.setDefaultCollection(defaultCollection);
			// cloudSolrServer.connect();
		}
		return cloudSolrServer;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println(getCloudSolrServer());
		CloudSolrServer cloudSolrServer = getCloudSolrServer();

		int num = 0;
		SolrQuery solrQuery = new SolrQuery("*:*");
		try {
			QueryResponse response = cloudSolrServer.query(solrQuery);
			SolrDocumentList docs = response.getResults();
			num = (int) docs.getNumFound();
		} catch (IOException | SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(num);
	}

}
