package com.olapdb.obase.data.index;

import com.olapdb.obase.data.Bytez;
import com.olapdb.obase.data.RecommendEntity;
import com.olapdb.obase.data.Tag;
import com.olapdb.obase.utils.VectorUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Vector;

public class Recommender {
	private Table table;
	private List<Tag> tags;
	private List<byte[]> rows;

	public Recommender(Table table){
		this.table = table;
	}
	public Recommender(Table table, List<byte[]> rows){
		this.table = table;
		this.rows = rows;
	}

	public Recommender similar(List<Tag> entity){
		this.tags = entity;
		return this;
	}

	public List<byte[]> top(int count){
		List<byte[]> finds = list();
		if(finds == null)
			return null;

		System.out.println("total item count = " + finds.size());

		List<byte[]> rets = new Vector<byte[]>();

		count = Math.min(count, finds.size());
		for(int i= 0 ; i<count; i++){
			rets.add(finds.get(i));
		}

		return rets;
	}

	public List<byte[]> list(){
		if(tags == null)
			return null;

		if(rows == null)
			return listByScan();

		return listByRows();
	}

	private List<byte[]> listByScan(){
		Scan scan = new Scan();
		Filter kof = new KeyOnlyFilter();
		scan.setFilter(kof);

		List<Match> matchs = new Vector<Match>();
		ResultScanner rs = null;
		try {
			rs = table.getScanner(scan);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}

		Lunnar lunar = new Lunnar(20);
		for(final Result r: rs){
			lunar.submit(new Runnable(){
				public void run(){
					TempEntity te = new TempEntity(r.getRow());
					Match match = new Match();
					match.row = r.getRow();
					match.similar = VectorUtil.calcSimilar(tags, te.getNormalTags());
					matchs.add(match);
				}
			});
		}
		lunar.waitForComplete();

		Collections.sort(matchs);

		List<byte[]> finds = new Vector<byte[]>();
		for(Match m : matchs){
			finds.add(m.row);
		}

		return finds;
	}
	private List<byte[]> listByRows(){
		List<Match> matchs = new Vector<Match>();
		Lunnar lunar = new Lunnar(20);
		for(final byte[] row: rows){
			lunar.submit(new Runnable(){
				public void run(){
					TempEntity te = new TempEntity(row);
					if(!te.needConnect()){
						Match match = new Match();
						match.row = row;
						match.similar = VectorUtil.calcSimilar(tags, te.getNormalTags());
						matchs.add(match);
					}else{
						System.out.println("TempEntity " + Bytez.toLong(row) + " need connect");
					}
				}
			});
		}
		lunar.waitForComplete();

		Collections.sort(matchs);

		List<byte[]> finds = new Vector<byte[]>();
		for(Match m : matchs){
			finds.add(m.row);
		}

		return finds;
	}

	private class TempEntity extends RecommendEntity{
		public TempEntity(byte[] row) {
			super(row);
		}

		@Override
		public Table getBasisTable() {
			return table;
		}
	}

	private class Match implements Comparable<Match>{
		public byte[] row;
		public double similar;
		@Override
		public int compareTo(Match o) {
			// TODO Auto-generated method stub
			if(similar > o.similar)
				return 1;
			else if(similar < o.similar)
				return -1;
			else
				return 0;
		}

	}
}
