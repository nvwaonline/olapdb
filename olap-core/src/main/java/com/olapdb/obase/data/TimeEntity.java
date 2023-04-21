package com.olapdb.obase.data;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Date;

public abstract class TimeEntity extends Entity {
	public TimeEntity(byte[] row) {
		super(row);

		if(needConnect()){
			this.setAttribute("createTime", Bytes.toBytes(System.currentTimeMillis()));
		}
	}

	public TimeEntity(byte[] row, boolean autoload) {
		super(row, autoload);

		if(needConnect()){
			this.setAttribute("createTime", Bytes.toBytes(System.currentTimeMillis()));
		}
	}

	public TimeEntity(Result r) {
		super(r);
	}

	public long getCreateTime(){
		return this.getAttributeAsLong("createTime", 0);
	}

	public Date getCreateDate(){
		return this.getAttributeAsDate("createTime");
	}
}
