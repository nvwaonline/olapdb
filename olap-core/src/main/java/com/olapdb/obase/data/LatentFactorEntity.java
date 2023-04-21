package com.olapdb.obase.data;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

public abstract class LatentFactorEntity extends RecommendEntity{
	public LatentFactorEntity(byte[] row){
		super(row);
	}

	public int getLatentFactorCount(){
		byte[] bytes = this.getAttribute("latentFactorCount");
		if(bytes == null)
			return 0;

		return Bytes.toInt(bytes);
	}

	public void setLatentFactorCount(int count){
		this.setAttribute("latentFactorCount", Bytes.toBytes(count));
	}

	public double[] getLatentFactors() {
		int count = this.getLatentFactorCount();
		double[] ret = new double[count];

		try {
			byte[] bytes = this.getAttribute("latentFactor");

			if(bytes != null){
				ByteArrayInputStream bios = new ByteArrayInputStream(bytes);
				DataInputStream dis = new DataInputStream(bios);
				for(int i=0; i<count; i++){
					ret[i] = dis.readDouble();
				}

				dis.close();
			}else{
				for(int i=0; i<count; i++){
					ret[i] = Math.random();
				}
				this.setLatentFactors(ret);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return ret;
	}

	public void setLatentFactors(double[] values){
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);

		try {
			for(double v: values){
				dos.writeDouble(v);
			}
			dos.close();
			this.setAttribute("latentFactor", baos.toByteArray());;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public double getAdjust(){
		byte[] bytes = this.getAttribute("adjust");
		if(bytes == null)
			return 0;
		return Bytes.toDouble(bytes);
	}

	public void setAdjust(double value){
		this.setAttribute("adjust", Bytes.toBytes(value));
	}
}
