package com.CCB.util.solr;


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;

/**
 * @Date	2015-11-26
 * @function	Solr工具类
 * @author ljj
 *1:int getQueryNum(String query)	根据查询语句查询 返回结果记录总数，用于分页
 *2:<T> List<T> getResultsByQuery(Class<T> clazz,String query,int pageIndex,int pageSize,String[] highligthFields,Map<String, Boolean> orderFields)
 *		根据查询语句查询	返回结果集对象
 *3:void clearIndexByQuery(String query)	根据查询语句查询 清除索引
 *4:SolrInputDocument beanToDoc(Object o)	把普通对象转化为SolrDocument对象
 *5:<T> T docToBean(Class<T> clazz,SolrDocument doc)	把SolrDocument对象转化为普通对象
 *6:<T> List<T> docToBean(Class<T> clazz,SolrDocumentList docs)	把SolrDocument对象集合转化为普通对象集合
 *7:void addBean(Object o)	给普通对象创建索引
 *8:void addBeans(List<Object> os)	给普通对象集合创建索引
 *9:void clearAllQuery()	清空索引数据库
 *10:void deleteById(String id)	根据rowkey删除索引
 *11:void deleteById(List<String> ids) 根据rowkey集合删除索引
 *12:String getDateTime(String dateString)	获取日期字符串
 */
public class SolrUilts {

//	private static CloudSolrServer solrServer = SolrCloudServer.getCloudSolrServer();
			
	private static HttpSolrServer solrServer = SolrServer.getSolrServer();
	private static int highlightSnippets = 3;	//设置分片数量为3
	private static int highlightFragsize = 200;	//设置分片大小为200
	
	
	/**
	 * 根据查询语句返回记录总数
	 * @param query		查询语句
	 * @return
	 */
	public static int getQueryNum(String query){
		int num = 0;
		SolrQuery solrQuery = new SolrQuery(query);
		try {
			QueryResponse response = solrServer.query(solrQuery);
			SolrDocumentList docs = response.getResults();
			num = (int)docs.getNumFound();
		} catch (IOException | SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return num;
	}
	
	/**
	 * 返回对象集合
	 * @param clazz		要返回的对象类
	 * @param query		查询语句，需要用户提前拼接好
	 * @param pageIndex		用于分页
	 * @param pageSize		设置分页大小
	 * @param highligthFields	高亮字段
	 * @param orderFields		k:排序字段 	v:true 降序排序	false 升序排序 	
	 * @return
	 */
	public static <T> List<T> getResultsByQuery(Class<T> clazz,String query,int pageIndex,int pageSize,String[] highligthFields,Map<String, Boolean> orderFields){
		
		ArrayList<T> solrList = new ArrayList<>();
		SolrQuery solrQuery = new SolrQuery(query);
		String k ="";
		Boolean v ;
		//排序  
//		solrQuery.setSort("title",ORDER.desc);
		if(null != orderFields){
			Iterator iterator = orderFields.keySet().iterator();
			while(iterator.hasNext()){
				k = iterator.next().toString();
				v = orderFields.get(k);
				if(v){
					solrQuery.setSort(k,ORDER.desc);
				}else{
					solrQuery.setSort(k,ORDER.asc);
				}
			}
		}
		
		//分页参数
		solrQuery.setStart(pageSize*(pageIndex-1));
		solrQuery.setRows(pageSize);
		
		//高亮设置
		solrQuery.setHighlight(true);	//开启高亮组件
		
		//高亮字段设置
//		solrQuery.addHighlightField("");
		if(null != highligthFields){
			for(String highlight:highligthFields){
				solrQuery.addHighlightField(highlight);
			}
		}
		
		solrQuery.setHighlightSimplePre("<span class='higthlight'>");	//标记高亮前缀		//这里的class属性需要在前台用css来控制显示效果
		solrQuery.setHighlightSimplePost("</span>");	//标记高亮后缀
		/*
		 * 设置高亮分片数
		 * 一般搜索词可能分布在文章中的不同位置，其所在一定长度的语句即为一个片段，默认为1
		 * 但根据业务需要有时候多取几个分片
		 */
		solrQuery.setHighlightSnippets(highlightSnippets);	
		
		solrQuery.setHighlightFragsize(highlightFragsize);	//设置分片大小，默认为100,如果太小高亮显示不全
		solrQuery.set("hl.highlightMultiTerm", "false");//禁止模糊高亮   很重要
		solrQuery.set("hl.mergeContiguous", "true");
		
		
		try {
			QueryResponse response = solrServer.query(solrQuery);
			Map<String,Map<String,List<String>>> highlightMap=response.getHighlighting();
			SolrDocumentList docs = response.getResults();
			List<String> list = new ArrayList<String>();;
			
			//拼接高亮分片内容
			for(SolrDocument doc : docs){
				String rowkey = doc.getFieldValue("rowkey").toString();
				for(String field : highligthFields ){
					list = highlightMap.get(rowkey).get(field);
					String str = "";
					if(null!=list){
						for(int j=0;j<list.size()&&j<highlightSnippets;j++){
							str = (j+1)+"、"+list.get(j)+"......<br/>";
						}
						doc.setField(field, str);
					}
				}
			}

			return docToBean(clazz,docs);
			
		} catch (IOException|SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		
	}
	
	/**
	 * 返回对象集合
	 * @param clazz		要返回的对象类
	 * @param query		查询语句，需要用户提前拼接好
	 * @param pageIndex		用于分页
	 * @param pageSize		设置分页大小
	 * @return
	 */
	public static <T> List<T> getResultsByQuery(Class<T> clazz,String query,int pageIndex,int pageSize){
		
		ArrayList<T> solrList = new ArrayList<>();
		SolrQuery solrQuery = new SolrQuery(query);
	
		
		//分页参数
		solrQuery.setStart(pageSize*(pageIndex-1));
		solrQuery.setRows(pageSize);
		
			
		try {
			QueryResponse response = solrServer.query(solrQuery);
			SolrDocumentList docs = response.getResults();
			return docToBean(clazz,docs);
			
		} catch (IOException|SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			sleep();
		}
		return null;
		
	}
	/**
	 * 根据query清除索引
	 * @param query
	 */
	public static void clearIndexByQuery(String query){
		try {
			solrServer.deleteByQuery(query);
			solrServer.commit(true, true);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			sleep();
		}
	}
	

	
	/**
	 * 对象转化为SolrInputDocument对象
	 * @param o
	 * @return
	 */
	public static SolrInputDocument beanToDoc(Object o){
		DocumentObjectBinder binder = new DocumentObjectBinder();
		return binder.toSolrInputDocument(o);
	}
	
	/**
	 * SolrDocument对象转化为对象
	 * @param clazz
	 * @param doc
	 * @return
	 */
	public static <T> T docToBean(Class<T> clazz,SolrDocument doc){
		DocumentObjectBinder binder = new DocumentObjectBinder();
		return binder.getBean(clazz, doc);
	}
	
	/**
	 * SolrDocumentList对象集合转化为对象集合
	 * @param clazz
	 * @param docs
	 * @return
	 */
	public static <T> List<T> docToBean(Class<T> clazz,SolrDocumentList docs){
		DocumentObjectBinder binder = new DocumentObjectBinder();
		return binder.getBeans(clazz, docs);
	}

	
	/**
	 * 添加单个对象
	 * @param o
	 */
	public static void addBean(Object o){
		try {
			UpdateResponse response = solrServer.addBean(o);
			solrServer.commit(true, true);
			System.out.println("time: "+response.getQTime()+"ms");
			System.out.println("Status: "+response.getStatus());
		} catch (IOException | SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			sleep();
		}
	}
	
	/**
	 * 添加对象集合
	 * @param <T>
	 * @param os
	 */
	public static <T> boolean addBeans(List<T> os){
		try {
			UpdateResponse response = solrServer.addBeans(os);
			solrServer.commit(true, true);
//			System.out.println("time: "+response.getQTime()+"ms");
//			System.out.println("Status: "+response.getStatus());
			return true;
		} catch (IOException | SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			//如果遇到SolrServer无法响应的情况，那么就需要等待
			sleep();
			return false;
		}
	}

	public static void sleep() {
		Thread t = new Thread();
		try {
			System.out.println("SolrServer无法响应...休息1s...");
			t.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 清除所有的索引
	 */
	public static void clearAllQuery(){
		try {
			String query = "*:*";
			solrServer.deleteByQuery(query);
			solrServer.commit(true, true);
			System.out.println("clear all the solr index seccess");
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			sleep();
		}
	}
	
	/**
	 * 根据id主键删除记录
	 * @param Id
	 */
	public void deleteById(String id){
		try {
			UpdateResponse response = solrServer.deleteById(id);	
			solrServer.commit(true, true);
			System.out.println("delete by id :"+id+" success!");
			System.out.println("time: "+response.getQTime()+"ms");
			System.out.println("Status: "+response.getStatus());
		} catch (SolrServerException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			sleep();
		}
	}
	
	
	/**
	 * 根据id主键删除记录
	 * @param Id
	 */
	public void deleteById(List<String> ids){
		try {
			UpdateResponse response = solrServer.deleteById(ids);	
			solrServer.commit(true, true);
			System.out.println("delete by ids  success!");
			System.out.println("time: "+response.getQTime()+"ms");
			System.out.println("Status: "+response.getStatus());
		} catch (SolrServerException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			sleep();
		}
	}
	
	
	
	public String getDateTime(String dateString){
		TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date;
		try {
			date = sdf.parse(dateString+" 24:00:00");
			sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"); 
			return sdf.format(date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("time convert failure");
		}
		return "*";
	}
	
	public void queryCase(){
		//AND
		SolrQuery params = new SolrQuery("name:apple AND manu:inc");
		//OR
		params.setQuery("name:apple OR manu:apache");
		//空格 等同于OR
		params.setQuery("name:apple manu:dell");
		
		params.setQuery("name:apple - manu:dell");
		
		params.setQuery("name:apple + manu:dell");
		//查询name包含solr apple
		params.setQuery("name:solr,apple");
		//manu不包含inc
		params.setQuery("name:solr,apple NOT ,manu:inc");
		//50<=price<=200
		params.setQuery("price:[50 TO 200]");
		params.setQuery("popularity:[5 TO 6]");
		params.setQuery("price:[50 TO 200] - popularity:[5 TO 6]");
		params.setQuery("price:[50 TO 200] + popularity:[5 TO 6]");
		
		//50<=price<=200 AND 5<=popularity<=6
		params.setQuery("price:[50 TO 200] AND popularity:[5 TO 6]");
		params.setQuery("price:[50 TO 200] OR popularity:[5 TO 6]");
		
		//过滤器查询，可以提高性能 filter类似多个条件组合 如and
		params.addFilterQuery("id:VA902B");
		params.addFilterQuery("price:[50 TO 200]");
		params.addFilterQuery("popularity:[* TO 5]");
		params.addFilterQuery("weight:*");
		
		//0<popularity<6 不包括等于
		params.addFacetQuery("popularity:{0 TO 6}");
		
		//排序
		params.addSort("id", ORDER.desc);
//		params.set("sort", "id desc");
		
		//分页：start开始页，rows每页显示记录条数
		params.add("start", "0");
		params.add("rows", "200");
		params.setStart(0);
		params.setRows(200);
		
		
		//设置高亮
		params.setHighlight(true);	//开启高亮组件
		params.addHighlightField("name");	//高亮字段
		params.setHighlightSimplePre("font color='red'");	//标记，高亮关键字前缀
		params.setHighlightSimplePost("</font>");			//标记，高亮关键字后缀
		params.setHighlightSnippets(1);		//结果分片数，默认为1
		params.setHighlightFragsize(1000);	//每个分片的最大长度  默认为100
		
		//分片信息
		params.setFacet(true).
			setFacetMinCount(1).
			setFacetLimit(5).
			addFacetField("name").
			addFacetField("inStock");
		
		try {
			QueryResponse response = solrServer.query(params);
			//输出查询结果集
			SolrDocumentList list = response.getResults();
			System.out.println("query result nums: "+list.getNumFound());
			for(int i=0;i<list.size();i++){
				System.out.println(list.get(i));
			}
			
			//分片查询在某些统计关键字的时候  可以统计关键字出现的次数，可以通过统计的关键字来搜索相关文章的信息
			//输出分片信息
			List<FacetField> facets = response.getFacetFields();
			for(FacetField facet : facets){
				List<Count> facetFields = facet.getValues();
				for(FacetField.Count count : facetFields){
					//关键字出现次数
					System.out.println(count.getCount());
				}
			}
			
			FacetField facets1 = response.getFacetField("name");
			for(FacetField facet : facets){
				List<Count> facetFields = facet.getValues();
				for(FacetField.Count count : facetFields){
					//关键字出现次数
					System.out.println(count.getCount());
				}
			}
		} catch (IOException|SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	
	/**
	 * 添单个SolrInputDocument对象
	 * @param doc
	 */
	public static void addDoc(SolrInputDocument doc){
		
		UpdateResponse response;
		try {
			response = solrServer.add(doc);
			solrServer.commit(true, true);
			System.out.println("time: "+response.getQTime()+"ms");
			System.out.println("Status: "+response.getStatus());
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	/**
	 * 添加SolrInputDocument对象集合
	 * @param docs
	 */
	public static void addDocs(Collection<SolrInputDocument> docs){

		UpdateResponse response;
		try {
			response = solrServer.add(docs);
			solrServer.commit(true, true);
			System.out.println("time: "+response.getQTime()+"ms");
			System.out.println("Status: "+response.getStatus());
		} catch (SolrServerException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 查询所有结果
	 */
	public static void queryAll(){
		ModifiableSolrParams params = new ModifiableSolrParams();
		params.set("q", "*:*");		//设置查询语句
		params.set("start", 0);		//设置返回记录起始页
		params.set("rows",10);	//设置返回记录条数
		params.set("rows",Integer.MAX_VALUE);	//设置返回记录条数
		params.set("sort", "score desc");	//设置安装关键字进行排序
		params.set("f1", "*,score");	//返回信息 *为全部，这里是全部加上score，如果不加下面就不能使用score
		QueryResponse response;
		try {
			response = solrServer.query(params);
			SolrDocumentList list = response.getResults();	
			for(int i=0;i<list.size();i++){
				System.out.println(list.get(i));
			}
		} catch (IOException|SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
