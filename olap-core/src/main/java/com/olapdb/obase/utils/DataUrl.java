package com.olapdb.obase.utils;

import java.util.Base64;

public class DataUrl {
	//	data:,文本数据
	//	data:text/plain,文本数据
	//	data:text/html,HTML代码
	//	data:text/html;base64,base64编码的HTML代码
	//	data:text/css,CSS代码
	//	data:text/css;base64,base64编码的CSS代码
	//	data:text/javascript,Javascript代码
	//	data:text/javascript;base64,base64编码的Javascript代码
	//	data:image/gif;base64,base64编码的gif图片数据
	//	data:image/png;base64,base64编码的png图片数据
	//	data:image/jpeg;base64,base64编码的jpeg图片数据
	//	data:image/x-icon;base64,base64编码的icon图片数据
	public static String typeFrom(String dataUrl){
		if(dataUrl == null)return "";
		String encodingPrefix = "base64,";
		int startIndex = dataUrl.indexOf(encodingPrefix);
		if(startIndex <0)return "";
		switch(dataUrl.substring(0, startIndex)){
		case "data:image/gif;":
			return ".gif";
		case "data:image/png;":
			return ".png";
		case "data:image/jpeg;":
			return ".jpg";
		}

		return "";
	}


	public static byte[] dataFrom(String dataUrl){
		String encodingPrefix = "base64,";
		int contentStartIndex = dataUrl.indexOf(encodingPrefix) + encodingPrefix.length();
		return Base64.getDecoder().decode(dataUrl.substring(contentStartIndex));
	}

}
