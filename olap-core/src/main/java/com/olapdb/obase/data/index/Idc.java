package com.olapdb.obase.data.index;

import com.olapdb.obase.data.Bytez;
import com.olapdb.obase.data.Entity;
import com.olapdb.obase.data.RemoteObject;
import com.olapdb.obase.utils.LruMap;
import com.olapdb.obase.utils.Obase;
import com.olapdb.obase.utils.Util;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

public class Idc extends Entity{
	@SuppressWarnings("unused")
	private final static String tableName = "olapdb:idc";

	private static LruMap<String, Idc> idcCache = new LruMap<String, Idc>(10000);
	private Table refTable;//
	private String refColumn;

	synchronized public static Idc getInstance(Table table, String column){
		String key = combineTag(table, column);
		Idc idc = idcCache.get(key);
		if(idc == null) {
			idc = new Idc(table, column);
			idcCache.put(key, idc);
			idc.connect();
		}
		return idc;
	}

	private Idc(Table table, String column) {
		this(Bytez.from(combineTag(table, column)));
		this.refTable = table;
		this.refColumn = column;
	}
	public Idc(byte[] row) {
		super(row);
	}
	public Idc(Result r) {
		super(r);
	}

	public Table getRefTable(){
		return refTable;
	}

	public String getRefColumn(){
		return refColumn;
	}

	@Override
	public RemoteObject connect(){
		if(this.needConnect()){
			if(this.getId() == 0){
				this.setId(Util.getDiscreteIntID(this.getBasisTable(), "tagUnifyIdentify"));
			}

			super.connect();
		}

		return this;
	}

	public static String combineTag(Table table, String column){
		return table.getName().getNameWithNamespaceInclAsString()+"|" + column;
	}

	public List<Idx> getIdxs(byte[] startRow, int maxResult){
		Scan scan = new Scan(Bytez.from(this.getId()), Bytez.from(this.getId()+1));
		if(startRow != null){
			scan.setStartRow(startRow);
		}

		try {
			ResultScanner rs = Obase.getIdxTable().getScanner(scan);

			List<Idx> rows = new Vector<Idx>();
			for(Result r: rs){
				rows.add(new Idx(r));
				if(rows.size() >= maxResult )
					break;
			}

			return rows;
		} catch (IOException e) {
			e.printStackTrace();
			return new Vector<Idx>();
		}
	}

	public static List<Idc> getTableIdcs(Table table, byte[] startRow, int maxResult){
		String front = table.getName().getNameWithNamespaceInclAsString()+"|";
		Scan scan = new Scan(Bytes.toBytes(front), Bytez.next(Bytes.toBytes(front)));
//		Filter pf = new PrefixFilter(Bytes.toBytes(front));
//		scan.setFilter(pf);
		if(startRow != null){
			scan.setStartRow(startRow);
		}

		try {
			ResultScanner rs = Obase.getIdcTable().getScanner(scan);

			List<Idc> rows = new Vector<Idc>();

			for(Result r: rs){
				rows.add(new Idc(r));
				if(rows.size() >= maxResult )
					break;
			}

			return rows;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return new Vector<Idc>();
		}
	}

	@Override
	public Table getBasisTable() {
		return Obase.getIdcTable();
	}

	private void setId(int id){
		this.setAttribute(Obase.COLUMN_ID, Bytez.from(id));
	}
	public int getId(){
		byte[] bytes = this.getAttribute(Obase.COLUMN_ID);
		if(bytes == null)
			return 0;

		return Bytes.toInt(bytes);
	}

	public String getName(){
		return Bytez.toString(this.getRow());
	}

	public void deleteIndexes() throws Exception{
		System.out.println("Delete index beginning.....");
		Scan scan = new Scan(Bytez.from(this.getId()), Bytez.from(this.getId() + 1));
		ResultScanner rs = Obase.getIndexTable().getScanner(scan);

		int count = 0;
		List<Delete> deletes = new Vector<Delete>();
		for(Result r: rs){
			deletes.add(new Delete(r.getRow()));
			if(deletes.size() >= 10000){
				Obase.getIndexTable().delete(deletes);
				deletes.clear();
				count++;
				System.out.println("Delete index: " + count*10000);
			}
		}

		Obase.getIndexTable().delete(deletes);
		System.out.println("Delete index ended");
	}

	public void deleteIdxes() throws Exception{
		System.out.println("Delete idx beginning.....");
		Scan scan = new Scan(Bytez.from(this.getId()), Bytez.from(this.getId() + 1));
		ResultScanner rs = Obase.getIdxTable().getScanner(scan);

		int count = 0;
		List<Delete> deletes = new Vector<Delete>();
		for(Result r: rs){
			deletes.add(new Delete(r.getRow()));
			if(deletes.size() >= 10000){
				Obase.getIdxTable().delete(deletes);
				deletes.clear();
				count++;
				System.out.println("Delete idx: " + count*10000);
			}
		}

		Obase.getIdxTable().delete(deletes);
		System.out.println("Delete idx ended");
	}
}
