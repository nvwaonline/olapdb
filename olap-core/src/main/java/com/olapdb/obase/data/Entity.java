package com.olapdb.obase.data;

import com.olapdb.obase.data.index.Indexer;
import com.olapdb.obase.data.index.Value;
import com.olapdb.obase.utils.Obase;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import javax.script.ScriptException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Vector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class Entity extends RemoteBasic {
	public Entity(byte[] row) {
		super(row, true);
	}

	public Entity(byte[] row, boolean autoload) {
		super(row, autoload);
	}

	public Entity(Result r) {
		super(r);
	}

	//////////////////////////////////////////////////////////////////////////////
	//Simple Attributes
	//////////////////////////////////////////////////////////////////////////////
	protected void addIndex(String column, Value value){
		addIndex(column, value, null);
	}
	protected void addIndex(String column, Value value, byte[] data){
		new Indexer(this).addIndex(this.getRow(), column, value, data).submit();;
	}

	protected void removeIndex(String column, Value value){
		new Indexer(this).removeIndex(this.getRow(), column, value).submit();
	}

	public void updateIndex(String column, Value oldVal, Value newVal){
		updateIndex(column, oldVal, newVal, null);
	}
	public void updateIndex(String column, Value oldVal, Value newVal, byte[] data){
		if(this.needConnect())return;//只有连接数据库的数据才能更新索引
		new Indexer(this).replaceIndex(this.getRow(), column, oldVal, newVal, data);
	}
	public void setIndexData(String column, Value value, byte[] data){
		new Indexer(this).setIndexData(this.getRow(), column, value, data);
	}

	//////////////////////////////////////////////////////////////////////////////
	//Simple Attributes
	//////////////////////////////////////////////////////////////////////////////
	public byte[] getAttribute(String name){
		return this.get(Bytes.toBytes(name));
	}
	public String getAttributeAsString(String name){
		return getAttributeAsString(name, "");
	}
	public String getAttributeAsString(String name, String defaultValue){
		byte[] bytes = this.getAttribute(name);
		if (bytes == null)
			return defaultValue;
		else
			return Bytez.toString(bytes);
	}
	public long getAttributeAsLong(String name){
		return getAttributeAsLong(name, 0);
	}
	public long getAttributeAsLong(String name, long defaultValue){
		byte[] bytes = this.getAttribute(name);
		if (bytes == null)
			return defaultValue;
		else
			return Bytez.toLong(bytes);
	}
	public int getAttributeAsInt(String name){
		return getAttributeAsInt(name, 0);
	}
	public int getAttributeAsInt(String name, int defaultValue){
		byte[] bytes = this.getAttribute(name);
		if (bytes == null)
			return defaultValue;
		else
			return Bytez.toInt(bytes);
	}
	public boolean getAttributeAsBoolean(String name){
		return getAttributeAsBoolean(name, false);
	}
	public boolean getAttributeAsBoolean(String name, boolean defaultValue){
		byte[] bytes = this.getAttribute(name);
		if (bytes == null)
			return defaultValue;
		else
			return Bytez.toBoolean(bytes);
	}
	public double getAttributeAsDouble(String name){
		return getAttributeAsDouble(name, 0);
	}
	public double getAttributeAsDouble(String name, double defaultValue){
		byte[] bytes = this.getAttribute(name);
		if (bytes == null)
			return defaultValue;
		else
			return Bytez.toDouble(bytes);
	}
	public float getAttributeAsFloat(String name){
		return getAttributeAsFloat(name, 0);
	}
	public float getAttributeAsFloat(String name, float defaultValue){
		byte[] bytes = this.getAttribute(name);
		if (bytes == null)
			return defaultValue;
		else
			return Bytez.toFloat(bytes);
	}
	public Date getAttributeAsDate(String name){
		return new Date(this.getAttributeAsLong(name));
	}
	public Timestamp getAttributeAsTimestamp(String name){
		return new Timestamp(this.getAttributeAsLong(name));
	}

	public void setAttribute(String name, byte[] value){
		this.set(Bytes.toBytes(name) , value);
	}

	public void setAttributes(String[] name, byte[][] value){
		try {
			byte[][] columns = new byte[name.length][];
			for(int i=0; i<name.length; i++){
				columns[i] = Bytes.toBytes(name[i]);
			}
			this.set(columns , value);
		} catch (ScriptException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void deleteAttribute(String name){
		try {
			this.delete(Bytes.toBytes(name));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void deleteAttributes(List<String> names){
		try {
			byte[][] columns = new byte[names.size()][];
			int len = names.size();
			for(int i=0; i<len; i++){
				columns[i] = Bytes.toBytes(names.get(i));
			}
			this.delete(columns);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void deleteAttributes(String name, List<String> items){
		List<String> names = new Vector<String>();
		for(String item : items){
			names.add(name+":"+item);
		}

		deleteAttributes(names);
	}
	public void deleteAttributeItems(String name){
		List<String> items = this.getAttributeItems(name);
		this.deleteAttributes(name, items);
	}

	public Put collectAttribute(String name){
		return this.collect(Bytes.toBytes(name));
	}
	public Put collectAttribute(String name, String item){
		return this.collect(Bytes.toBytes(name+":"+item));
	}

	//////////////////////////////////////////////////////////////////////////////
	//Array Attributes
	//////////////////////////////////////////////////////////////////////////////
	public byte[] getAttribute(String name, String item){
		return this.get(Bytes.toBytes(name+":"+item));
	}
	public String getAttributeAsString(String name, String item, String defaultValue){
		byte[] bytes = this.getAttribute(name, item);
		if (bytes == null)
			return defaultValue;
		else
			return Bytez.toString(bytes);
	}
	public long getAttributeAsLong(String name, String item){
		return getAttributeAsLong(name, item, 0);
	}
	public long getAttributeAsLong(String name, String item, long defaultValue){
		byte[] bytes = this.getAttribute(name, item);
		if (bytes == null)
			return defaultValue;
		else
			return Bytez.toLong(bytes);
	}
	public int getAttributeAsInt(String name, String item){
		return getAttributeAsInt(name, item, 0);
	}
	public int getAttributeAsInt(String name, String item, int defaultValue){
		byte[] bytes = this.getAttribute(name, item);
		if (bytes == null)
			return defaultValue;
		else
			return Bytez.toInt(bytes);
	}
	public boolean getAttributeAsBoolean(String name, String item){
		return getAttributeAsBoolean(name, item, false);
	}
	public boolean getAttributeAsBoolean(String name, String item, boolean defaultValue){
		byte[] bytes = this.getAttribute(name, item);
		if (bytes == null)
			return defaultValue;
		else
			return Bytez.toBoolean(bytes);
	}
	public double getAttributeAsDouble(String name, String item){
		return getAttributeAsDouble(name, item, 0);
	}
	public double getAttributeAsDouble(String name, String item, double defaultValue){
		byte[] bytes = this.getAttribute(name, item);
		if (bytes == null)
			return defaultValue;
		else
			return Bytez.toDouble(bytes);
	}
	public float getAttributeAsFloat(String name, String item){
		return getAttributeAsFloat(name, item, 0);
	}
	public float getAttributeAsFloat(String name, String item, float defaultValue){
		byte[] bytes = this.getAttribute(name, item);
		if (bytes == null)
			return defaultValue;
		else
			return Bytez.toFloat(bytes);
	}
	public void setAttribute(String name, String item, byte[] value){
		this.set(Bytes.toBytes(name+":"+item) , value);
	}

	public void setAttribute(String name, String[] item, byte[][] value){
		try {
			byte[][] columns = new byte[item.length][];
			for(int i=0; i<item.length; i++){
				columns[i] = Bytes.toBytes(name+":"+item[i]);
			}
			this.set(columns , value);
		} catch (ScriptException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void deleteAttribute(String name, String item){
		try {
			this.delete(Bytes.toBytes(name+":"+item));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public List<String> getAttributeItems(String name){
		String prefix = name+":";
		int pos = prefix.length();

		return this.getBasis().keySet().stream()
				.map(t->Bytez.toString(t))
				.filter(t->t.startsWith(prefix))
				.map(t->t.substring(pos))
				.collect(Collectors.toList());
	}

	public static Stream<Result> stream(Class entityClass){
		return stream(entityClass, new Scan());
	}
	public static Stream<Result> stream(Class entityClass, Scan scan){
		return singleScanStream(entityClass, scan);
//		return multiScanStream(entityClass, scan, 100);
	}

	public static Stream<Result> singleScanStream(Class entityClass, Scan scan){
		try{
			ResultScanner rs = Obase.getTable(entityClass).getScanner(scan);
			return StreamSupport.stream(rs.spliterator(), false);
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}

	public static Stream<Result> multiScanStream(Class entityClass, Scan scan, int threads){
		try{
			ResultScanner rs = new ParallelScanner(entityClass, scan, threads);
			return StreamSupport.stream(rs.spliterator(), false);
		}catch(Exception e){
			return null;
		}
	}

	public static Optional<Result> next(Class entityClass, byte[] row){
		Scan scan = new Scan();
		if(row!=null){
			scan.withStartRow(row);
		}
		scan.setOneRowLimit();
//		scan.setCaching(1);
//		scan.setLimit(1);
		return stream(entityClass, scan).findFirst();
	}

	public static Optional<Result> before(Class entityClass, byte[] row){
		Scan scan = new Scan().withStopRow(row);
		scan.setOneRowLimit();
//		scan.setLimit(1);
		scan.setReversed(true);
		return stream(entityClass, scan).findFirst();
	}

	public static long scanRowCount(Class entityClass){
		Scan scan = new Scan();
		FirstKeyOnlyFilter fkof = new FirstKeyOnlyFilter();
		scan.setFilter(fkof);
		return singleScanStream(entityClass, scan).count();
	}
	public static long scanRowCountFast(Class entityClass){
		Scan scan = new Scan();
		FirstKeyOnlyFilter fkof = new FirstKeyOnlyFilter();
		scan.setFilter(fkof);
		return multiScanStream(entityClass, scan, 100).count();
	}
}
