package com.olapdb.obase.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.olapdb.obase.data.Bytez;
import com.olapdb.obase.data.Entity;
import com.olapdb.obase.data.RemoteBasic;
import com.olapdb.obase.data.index.Indexer;
import com.olapdb.obase.data.index.Lunnar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class Obase {
	public static final ObjectMapper mapper = new ObjectMapper();

	public static String SERVER = "";

	//通用的列簇名称
	public final static String FAMILY_ATTR = "A"; //attributes 属性 对象的基本属性
	public final static String FAMILY_EXEC = "E"; //execute 命令执行 row, method, para
	public final static String FAMILY_EXEC_REGION = "R"; //execute 执行region操作命令

	public final static String COLUMN_REFERENCE = "reference";
	public final static String COLUMN_ID = "id";

	//全局表
	private final static String TABLE_INCREMENT = "olapdb:increment"; //全局表
	private final static String TABLE_IDCS	  	= "olapdb:idc"; //列表，每个列值分配一个long cid; row组成：table|column
	private final static String TABLE_IDXS	  	= "olapdb:idx"; //标签表，row组成：cid|value.ranking
	private final static String TABLE_INDEXS  	= "olapdb:index"; //索引表，row组成：cid|value.ranking|row;
	//data域：null 或者 count/total用于全文索引权重计算

	private static Connection conn;
	private static String Namespace = "";
	private static boolean Restrict= true;

	public static boolean Debug = false; //调试
	public static boolean DebugIndex = false; //索引调试

//	private final static Algorithm DefaultAlgorithm = null;
	private final static Algorithm DefaultAlgorithm = Algorithm.LZ4;
//	private final static Algorithm DefaultAlgorithm = Algorithm.ZSTD;

	public final static Scan FAST_SCAN = new Scan().setFilter(new FirstKeyOnlyFilter());

	public static void setQuorum(String server, String namespace, boolean restrict){
		System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");

		System.out.println("setQuorum server = " + server + " namespace = " + namespace);

		Obase.SERVER = server;
		Obase.Namespace = namespace;
		Obase.Restrict = restrict;

		conn = null;

		Configuration HBASE_CONFIG = new Configuration();
		HBASE_CONFIG.set("hbase.zookeeper.quorum", Obase.SERVER);
		HBASE_CONFIG.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 120000);

		Configuration conf = HBaseConfiguration.create(HBASE_CONFIG);
		try {
			conn = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	//connection
	public static Connection getConnection(){
		return conn;
	}

	//global table
	public static Table getIncrementTable(){
		try {
			return conn.getTable(TableName.valueOf(Obase.TABLE_INCREMENT));
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}

	public static Table getIdcTable(){
		try {
			return conn.getTable(TableName.valueOf(Obase.TABLE_IDCS));
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}
	public static Table getIdxTable(){
		try {
			return conn.getTable(TableName.valueOf(Obase.TABLE_IDXS));
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}
	public static Table getIndexTable(){
		try {
			return conn.getTable(TableName.valueOf(Obase.TABLE_INDEXS));
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}

	private static Table getTable(String tname){
		try{
			String name = tname.indexOf(":")>0?tname:Namespace+":"+tname;
			return conn.getTable(TableName.valueOf(name));
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}

	public static Table getTable(RemoteBasic entity){
		return getTable(entity.getClass());
	}

	public static Table getTable(Class entityClass){
		try{
			Field field = entityClass.getDeclaredField("tableName");
			field.setAccessible(true);
			String tableName = (String)field.get(entityClass);
			return getTable(tableName);
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}

	public static Table getTable(String namespace, String tname){
		if(Restrict && !Obase.Namespace.equals(namespace)){
			throw new RuntimeException("The access for " + namespace + ":"+tname + " is limited. Now only the namespace " + Namespace + " is avaliable.");
		}

		try{
			String name = namespace+":"+tname;
			return conn.getTable(TableName.valueOf(name));
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}

	public static boolean tableExist(Class entityClass)throws Exception{
		try{
			Field field = entityClass.getDeclaredField("tableName");
			field.setAccessible(true);
			String tableName = (String)field.get(entityClass);
			return tableExist(tableName);
		}catch(Exception e){
			e.printStackTrace();
			return false;
		}
	}

	public static boolean tableExist(String tableName)throws Exception{
		return conn.getAdmin().tableExists(TableName.valueOf(tableName));
	}

	public static void createTable(String tbName, int maxVersions, int timeToLive, boolean inMemory, String observer, Algorithm type) throws Exception{
		Admin admin = conn.getAdmin();
		TableName tName = TableName.valueOf(Namespace+":"+tbName);
		deleteTable(tName);

		HTableDescriptor desc=new HTableDescriptor(tName);
		//添加列簇  f, 存储文档信息
		HColumnDescriptor hcd = new HColumnDescriptor(Obase.FAMILY_ATTR);//doc
		hcd.setMaxVersions(maxVersions);
		hcd.setInMemory(inMemory);
		if(timeToLive >0){
			hcd.setTimeToLive(timeToLive);
		}
		if(type !=null){
			hcd.setCompressionType(type);
		}
		desc.addFamily(hcd);

		if(observer != null){
			hcd = new HColumnDescriptor(Obase.FAMILY_EXEC);//doc
			hcd.setMaxVersions(1);
			desc.addFamily(hcd);
			desc.addCoprocessor(observer);
		}

		admin.createTable(desc);
	}

	public static void createTable(String tbName, int maxVersions, int timeToLive, boolean inMemory, String observer, Algorithm type, byte[][] splits) throws Exception{
		Admin admin = conn.getAdmin();
		TableName tName = TableName.valueOf(tbName);
		deleteTable(tName);

		HTableDescriptor desc=new HTableDescriptor(tName);
		//添加列簇  f, 存储文档信息
		HColumnDescriptor hcd = new HColumnDescriptor(Obase.FAMILY_ATTR);//doc
		hcd.setMaxVersions(maxVersions);
		hcd.setInMemory(inMemory);
		if(timeToLive >0){
			hcd.setTimeToLive(timeToLive);
		}
		if(type !=null){
			hcd.setCompressionType(type);
		}
		desc.addFamily(hcd);

		if(observer != null){
			hcd = new HColumnDescriptor(Obase.FAMILY_EXEC);//doc
			hcd.setMaxVersions(1);
			desc.addFamily(hcd);
			desc.addCoprocessor(observer);
		}

		admin.createTable(desc, splits);
	}

	/**
	 * 获取obase唯一标志。如果不存在，则创建
	 */
	public static String olapObaseIdentify = "";
	public synchronized static String getIdentify(){
		if(olapObaseIdentify == null || olapObaseIdentify.isEmpty()) {
			String varName = "olapObaseIdentify";
			byte[] data = Util.getGlobalValue(varName);
			olapObaseIdentify = Bytez.toString(data);
		}

		return olapObaseIdentify;
	}

	/**
	 * 删除一个命名空间中的所有表格
	 * @param ns，命名空间
	 * @throws IOException
	 */
	public static void clearNamespaceAnyway(String ns) throws IOException {
		Admin admin = Obase.getConnection().getAdmin();
		for(TableName tn : admin.listTableNames()){
			if(! Bytes.equals(tn.getNamespace(), Bytes.toBytes(ns)))
				continue;

			deleteTable(tn);
		}
	}

	public static void clearNamespaceWithoutIndex(String ns) throws IOException {
		Admin admin = Obase.getConnection().getAdmin();
		for(TableName tn : admin.listTableNames()){
			if(! Bytes.equals(tn.getNamespace(), Bytes.toBytes(ns)))
				continue;

			String tableName = tn.getNameAsString();
			switch (tableName){
				case "olapdb:increment":
				case "olapdb:idx":
				case "olapdb:idc":
				case "olapdb:index":
					continue;
			}

			deleteTable(tn);
		}
	}

	public static void createTableWithNamespace(String tbName, int maxVersions, int timeToLive, boolean inMemory, Algorithm type) throws Exception{
		Admin admin = conn.getAdmin();
		TableName tName = TableName.valueOf(tbName);
		deleteTable(tName);

		HTableDescriptor desc=new HTableDescriptor(tName);
		//添加列簇  f, 存储文档信息
		HColumnDescriptor hcd = new HColumnDescriptor(Obase.FAMILY_ATTR);//doc
		hcd.setMaxVersions(maxVersions);
		hcd.setInMemory(inMemory);
		if(timeToLive <=0){
			timeToLive = Integer.MAX_VALUE;
		}
		if(type!=null) {
			hcd.setCompressionType(type);
		}
		hcd.setTimeToLive(timeToLive);
		desc.addFamily(hcd);

		admin.createTable(desc);
	}

	private static void deleteTable(TableName tbName) throws IOException{
		Admin admin = conn.getAdmin();
		if(admin.tableExists(tbName)) {
			Table table = conn.getTable(tbName);
			String tableName = tbName.getNameAsString();
			switch (tableName){
				case "olapdb:increment":
				case "olapdb:idx":
				case "olapdb:idc":
				case "olapdb:index":
					break;
				default:
					Util.clearTableCounters(table);
					break;
			}

			switch (tableName){
				case "olapdb:increment":
				case "olapdb:idx":
				case "olapdb:idc":
				case "olapdb:index":
					break;
				default:
					Indexer.destroyTableIndex(table);
					break;
			}

			if(admin.isTableEnabled(tbName)){
				admin.disableTable(tbName);
			}
			admin.deleteTable(tbName);
		}
	}

	/**
	 * 列出所有namespace
	 * @return
	 * @throws IOException
	 */
	public static void createNamespace(String name) throws IOException {
		NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(name);
		conn.getAdmin().createNamespace(builder.build());
	}

	/**
	 * 列出所有namespace
	 * @return
	 * @throws IOException
	 */
	public static NamespaceDescriptor[] listNamespace() throws IOException {
		NamespaceDescriptor[] list = conn.getAdmin().listNamespaceDescriptors();
		return list;
	}

	/**
	 * 判断一个namespace是否存在
	 * @param namespace
	 * @return
	 * @throws IOException
	 */
	public static boolean existsNamespace(String namespace) throws IOException {
		for (NamespaceDescriptor namespaceDescriptor : listNamespace()) {
			if (namespaceDescriptor.getName().equals(namespace)) {
				return true;
			}
		}
		return false;
	}

	public static void saveAll(Collection<? extends Entity> entityListInput){
		saveAll(entityListInput, 5, 500, null);
	}

	public static void saveAll(Collection<? extends Entity> entityListInput, Durability durability){
		saveAll(entityListInput, 5, 500, durability);
	}

	public static void saveAll(Collection<? extends Entity> entityListInput, int thread, int batch_size, Durability durability){
		System.out.println("saveAll size = " + entityListInput.size());
		List<Entity> entityList = new Vector<>();
		entityList.addAll(entityListInput);

		if(thread < 5)thread = 5;
		if(batch_size < 500)batch_size = 500;
		if(thread>20)thread = 20;
		if(batch_size > 10000)batch_size = 10000;

		int batchSize = batch_size;
		Lunnar lunnar = new Lunnar(thread);
		int batchNum = (entityList.size() + batchSize-1)/batchSize;
		for(int i=0; i<batchNum; i++){
			final int index = i;
			lunnar.submit(new Runnable() {
				@Override
				public void run() {
					saveAllMini(entityList.subList(index*batchSize, Math.min((index+1)*batchSize, entityList.size())), durability);
				}
			});
		}
		lunnar.waitForComplete();
	}

	private static void saveAllMini(Collection<Entity> entityList, Durability durability){
//		System.out.println("saveAllMini....." + entityList.size());
		entityList.stream().collect(Collectors.groupingBy(e->e.getClass())).forEach((K,V)->{
			List<Put> puts = V.stream().map(e->durability == null?e.collect():e.collect().setDurability(durability)).filter(e->e!=null).collect(Collectors.toList());
//			System.out.println("saveAllMini....." + puts.size());
			if(puts.size() > 0) {
				try {
					Obase.getTable(K).put(puts);
				} catch (Exception e) {
					e.printStackTrace();
					throw new RuntimeException("Hbase访问出错，请检测Hbase集群是否正常", e);
				}
			}
		});
	}

    public static void deleteAll(Collection<? extends Entity> entityListInput){
		System.out.println("deleteAll size = " + entityListInput.size());
		List<Entity> entityList = new Vector<>();
		entityList.addAll(entityListInput);

		Lunnar lunnar = new Lunnar(5);
		int batchSize = 1000;
		int batchNum = (entityList.size() + batchSize-1)/batchSize;
		for(int i=0; i<batchNum; i++){
			final int index = i;
			lunnar.submit(new Runnable() {
				@Override
				public void run() {
					deleteAllMini(entityList.subList(index*batchSize, Math.min((index+1)*batchSize, entityList.size())));
				}
			});
		}
		lunnar.waitForComplete();
    }

	private static void deleteAllMini(Collection<Entity> entityList){
//		System.out.println("deleteAllMini....." + entityList.size());
		entityList.stream().collect(Collectors.groupingBy(e->e.getClass())).forEach((K,V)->{
			List<Delete> deletes = V.stream().map(e->new Delete(e.getRow())).collect(Collectors.toList());
			if(deletes.size() > 0) {
				try {
					Obase.getTable(K).delete(deletes);
				} catch (Exception e) {
					e.printStackTrace();
					throw new RuntimeException("Hbase访问出错，请检测Hbase集群是否正常", e);
				}
			}
		});
	}

	public static List<HRegionLocation> getRegionsInRange(Class entityClass,
														  byte[] startKey, byte[] endKey,
														  boolean includeEndKey,
														  boolean reload) throws IOException {
		final boolean endKeyIsEndOfTable = Bytes.equals(endKey,HConstants.EMPTY_END_ROW);
		if ((Bytes.compareTo(startKey, endKey) > 0) && !endKeyIsEndOfTable) {
			throw new IllegalArgumentException(
					"Invalid range: " + Bytes.toStringBinary(startKey) +
							" > " + Bytes.toStringBinary(endKey));
		}
		List<HRegionLocation> regionsInRange = new ArrayList<>();
		byte[] currentKey = startKey;
		RegionLocator locator = conn.getRegionLocator(Obase.getTable(entityClass).getName());
		do {
			HRegionLocation regionLocation = locator.getRegionLocation(currentKey, reload);
			regionsInRange.add(regionLocation);
			currentKey = regionLocation.getRegion().getEndKey();
		} while (!Bytes.equals(currentKey, HConstants.EMPTY_END_ROW)
				&& (endKeyIsEndOfTable || Bytes.compareTo(currentKey, endKey) < 0
				|| (includeEndKey && Bytes.compareTo(currentKey, endKey) == 0)));
		return regionsInRange;
	}

	public static List<Result> get(Class entityClass, List<Get> gets) {
		try {
			Result[] results = Obase.getTable(entityClass).get(gets);
			return Arrays.stream(results)
					.filter(e -> e != null && e.getRow() != null)
					.collect(Collectors.toList());
		} catch (IOException e1) {
			e1.printStackTrace();
			throw new RuntimeException("Hbase访问出错，请检测Hbase集群是否正常", e1);
		}
	}

	public static void delete(Class entityClass, List<Delete> deletes) {
		try {
			Obase.getTable(entityClass).delete(deletes);
		} catch (IOException e1) {
			e1.printStackTrace();
			throw new RuntimeException("Hbase访问出错，请检测Hbase集群是否正常", e1);
		}
	}

	public static void createRegion(Class entityClass, byte[] startRow)throws Exception{
		createRegion(getTable(entityClass).getName(), startRow);
	}

	public static void createRegion(TableName tablename, byte[] startRow)throws Exception{
		Admin admin = conn.getAdmin();
		admin.split(tablename, startRow);
	}

	public static void splitRegion(byte[] regionName, byte[] startRow)throws Exception{
		Admin admin = conn.getAdmin();
		Future<Void> future = admin.splitRegionAsync(regionName, startRow);
	}

	public static void mergeRegion(byte[][] namesOfRegionToMerge)throws Exception{
		Admin admin = conn.getAdmin();
		admin.mergeRegionsAsync(namesOfRegionToMerge, false);
	}

	public static void flushRegion(byte[] regionName)throws Exception{
		Admin admin = conn.getAdmin();
		admin.flushRegion(regionName);
	}
	public static void compactRegion(byte[] regionName)throws Exception{
		Admin admin = conn.getAdmin();
		admin.compactRegion(regionName);
	}
	public static void moveRegion(byte[] regionName, ServerName serverName)throws Exception{
		Admin admin = conn.getAdmin();
		admin.move(regionName, Bytez.from(serverName.getServerName()));
	}

	public static Collection<ServerName> getServerNames()throws Exception{
		Admin admin = conn.getAdmin();
		return admin.getRegionServers();
	}
}
