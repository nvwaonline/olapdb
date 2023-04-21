package com.olapdb.obase.data;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public abstract class SearchableEntity extends ReferenceEntity {
	public SearchableEntity(byte[] row) {
		super(row);
	}

	public SearchableEntity(Result r) {
		super(r);
	}

	public int getWordCount(){
		byte[] bytes = this.getAttribute("wordCount");
		if(bytes == null)
			return 0;

		return Bytes.toInt(bytes);
	}

	public void setWordCount(int value){
		this.setAttribute("wordCount", Bytes.toBytes(value));
	}

	public long getShowTime(){
		byte[] bytes = this.getAttribute("showTime");
		if(bytes == null)
			return 0;

		return Bytes.toLong(bytes);
	}
	public void setShowTime(long value){
		this.setAttribute("showTime", Bytes.toBytes(value));
	}

	public long getChoiceTime(){
		byte[] bytes = this.getAttribute("choiceTime");
		if(bytes == null)
			return 0;

		return Bytes.toLong(bytes);
	}
	public void setChoiceTime(long value){
		this.setAttribute("choiceTime", Bytes.toBytes(value));
	}

//	public void setText

}
