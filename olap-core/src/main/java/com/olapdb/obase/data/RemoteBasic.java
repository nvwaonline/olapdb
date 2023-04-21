package com.olapdb.obase.data;

import com.olapdb.obase.utils.Obase;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import javax.script.ScriptException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

public abstract class RemoteBasic extends RemoteObject{
	private Map<byte[], byte[]> basis = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
	private byte[] row;
	private boolean connected = false;
	private boolean autoload;
	private long timestamp = 0;

	protected RemoteBasic(byte[] row , boolean autoload){
	  this.row = row;

	  //在创建的同时把数据从数据库加载到缓存
	  this.autoload = autoload;
	  if(autoload){
		  connected = loadall();
	  }
	}


	public RemoteBasic(Result r){
		this.row = r.getRow();

		for(Cell c : r.listCells()){
			if(Bytez.equals(CellUtil.cloneFamily(c), Bytez.from(Obase.FAMILY_ATTR))){
				basis.put(CellUtil.cloneQualifier(c), CellUtil.cloneValue(c));
			}
		}

		this.connected = true;
	}

	public boolean needConnect(){
		return this.connected == false;
	}
	public boolean isAutoload(){
		return this.autoload;
	}

	public byte[] getRow(){
//		return row.clone();
		return row;
	}

	public long getTimestamp(){
		return timestamp;
	}

	public String getRowAsBase64(){
		return Bytez.toBase64(row);
	}
	public static byte[] rowFromBase64(String base64){
		return Bytez.fromBase64(base64);
	}

	@Deprecated
	protected void updateRowKey(int start, int end, byte[] nbytes)throws Exception{
		if(!this.needConnect())
			throw new Exception("data not allow update");

		row = Bytez.add(Bytez.copy(row, 0, start)
				, nbytes
				, Bytez.copy(row, end)
		);
	}

	protected Map<byte[], byte[]> getBasis(){
		return this.basis;
	}

	public Table getBasisTable(){
		return Obase.getTable(this);
	}

	public RemoteObject connect(){
		if(this.connected)
			return this;

		try {
			saveall();
			this.connected = true;
//			Indexer.incRowCount(this.getBasisTable(),1);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Hbase access failed. Please check the HBase connection.", e);
		}

		return this;
	}

	public void disconnect(){
		this.connected = false;
	}

	public Put collect(){
		if(this.connected)
			return null;

		try {
			this.connected = true;
			return putall();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Hbase access failed. Please check the HBase connection.", e);
		}
	}

	public Put collect(byte[] column){
		if(this.connected)
			return null;

		try {
			return putColumn(column);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Hbase access failed. Please check the HBase connection.", e);
		}
	}

	public void delete(){
		Delete delete = new Delete(this.getRow());
		try {
			this.getBasisTable().delete(delete);
//			Indexer.decRowCount(this.getBasisTable());
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Hbase access failed. Please check the HBase connection.", e);
		}

		this.disconnect();
		//delete data index...
	}
	private void saveall() throws Exception{
		this.getBasisTable().put(putall());
	}
	private Put putall() throws Exception{
		if(this.getBasisTable() == null)
			throw new ScriptException("getBasisTable() == null");

		Put put = new Put(row);
		for(byte[] key : basis.keySet()){
			byte[] v = basis.get(key);
			put.addColumn(Bytez.from(Obase.FAMILY_ATTR), key, v );
		}
		timestamp = Math.max(timestamp, put.getTimestamp());

		return put;
	}

	private Put putColumn(byte[] column) throws Exception{
		if(this.getBasisTable() == null)
			throw new ScriptException("getBasisTable() == null");

		if(!basis.containsKey(column))return null;

		Put put = new Put(row);
		put.addColumn(Bytez.from(Obase.FAMILY_ATTR), column, basis.get(column) );
		timestamp = Math.max(timestamp, put.getTimestamp());

		return put;
	}

	private boolean loadall(){
		if(this.getBasisTable() == null)
			return false;

		try {
			Get get = new Get(this.row);
			get.addFamily(Bytez.from(Obase.FAMILY_ATTR));
			Result r = getBasisTable().get(get);
			if(r==null || r.isEmpty())
				return false;

			List<Cell> cells = r.listCells();
			if(cells == null)return false;
			for(Cell c : cells){
				basis.put(CellUtil.cloneQualifier(c), CellUtil.cloneValue(c));
				timestamp = Math.max(timestamp, c.getTimestamp());
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Hbase access failed. Please check the HBase connection.", e);
		}

		return true;
	}

	public void set(byte[] column, byte[] data){
		byte[] value = basis.get(column);

		if(value != null && Bytes.equals(data, value))
			return;

		value = data;

		basis.put(column, value);

		if(connected){
			saveColumnToTable(Obase.FAMILY_ATTR, column, value);
		}

		//update index
	}

	public void set(byte[][] columns, byte[][] data) throws ScriptException{
		if(columns.length != data.length)throw new ScriptException("columns[] and datas[] dimension not match.");

		int length = data.length;
		List<byte[]> clist = new Vector<byte[]>();
		List<byte[]> vlist = new Vector<byte[]>();
		for(int i=0; i<length; i++){
			byte[] column = columns[i];
			byte[] value = basis.get(column);

			if(value != null && Bytes.equals(data[i], value))
				continue;

			value = data[i];

			basis.put(column, value);
			clist.add(column);
			vlist.add(value);
		}

		if(connected){
			saveColumnToTable(Obase.FAMILY_ATTR, clist.toArray(new byte[0][0]), vlist.toArray(new byte[0][0]));

			//update index
		}
	}

	public byte[] get(byte[] column){
		return basis.get(column);
	}

	public void delete(byte[] column){
		if(!basis.containsKey(column))
			return;

		basis.remove(column);

		if(this.connected){
			this.deleteColumnFromTable(Obase.FAMILY_ATTR, column);
		}

		//update index
	}

	public void delete(byte[][] columns){
		for(byte[] column : columns){
			if(!basis.containsKey(column))
				return;
			basis.remove(column);
		}

		if(this.connected){
			this.deleteColumnFromTable(Obase.FAMILY_ATTR, columns);
		}

		//update index

	}


	private void saveColumnToTable(String family, byte[] column, byte[] v){
		if(row == null)return;

		Put put = new Put(this.getRow());
		put.addColumn(Bytez.from(family), column, v);

		try {
			this.getBasisTable().put(put);
			timestamp = Math.max(timestamp, put.getTimestamp());
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Hbase access failed. Please check the HBase connection.", e);
		}
	}

	private void saveColumnToTable(String family, byte[][] column, byte[][] v) throws ScriptException{
		//如果在服务器
		if(row == null)return;

		if(column == null || v == null || column.length != v.length)
			throw new ScriptException("Dimension not match");

		if(v.length==0)return;
		Put put = new Put(this.getRow());

		for(int i=0; i<v.length ; i++){
			put.addColumn(Bytez.from(family), column[i], v[i]);
		}

		try {
			this.getBasisTable().put(put);
			timestamp = Math.max(timestamp, put.getTimestamp());
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Hbase access failed. Please check the HBase connection.", e);
		}
	}

	private void deleteColumnFromTable(String family, byte[] column) {
		//如果在服务器
		if(row == null)return;

		Delete del = new Delete(this.getRow());
		del.addColumns(Bytez.from(family), column);

		try {
			this.getBasisTable().delete(del);
			timestamp = Math.max(timestamp, del.getTimestamp());
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Hbase access failed. Please check the HBase connection.", e);
		}
	}
	private void deleteColumnFromTable(String family, byte[][] columns) {
		//如果在服务器
		if(row == null)return;

		if(columns == null || columns.length==0)
			return;

		Delete del = new Delete(this.getRow());
		for(byte[] column : columns){
			del.addColumns(Bytez.from(family), column);
		}

		try {
			this.getBasisTable().delete(del);
			timestamp = Math.max(timestamp, del.getTimestamp());
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Hbase access failed. Please check the HBase connection.", e);
		}
	}
}
