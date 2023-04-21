package com.olapdb.obase.utils;

import com.olapdb.obase.data.Bytez;
import com.olapdb.obase.data.SearchableEntity;
import com.olapdb.obase.data.Tag;
import com.olapdb.obase.data.index.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

public class DocUtil {
	public static List<Elite> searchWords(Class entityClass, String words){
		Indexer indexer = new Indexer(entityClass);

		List<String> sws = Util.searchList(words);
		for(String w : sws)indexer.addTag("word", Value.from(w));
		List<Elite> elites = indexer.list();
		if(elites == null || elites.isEmpty()){
			indexer = new Indexer(entityClass);
			sws = Util.searchListSmart(words);
			for(String w : sws)indexer.addTag("word", Value.from(w));
			elites = indexer.list();
		}

		return elites;
	}

	public static void sortWithWords(Table hostTable, List<Elite> elites, int maxCount){
		long docCount = Indexer.getRowCount(hostTable);
		for(Elite e : elites){
			int total = 1;
			for(Find f : e.getMatchs()){
				if(f.getData()!=null){
					total = Math.max(total, Bytez.toInt(f.getData(), 4));
				}
			}

			for(Find f : e.getMatchs()){
				if(f.getIdx() !=null && f.getData()!=null){
					double weight = 0.01 + Math.log(docCount*1.0/ f.getIdx().getReference());
					e.setScore(e.getScore() + weight* Bytez.toInt(f.getData())/total);
				}
			}
		}

		Collections.sort(elites, new Comparator<Elite>(){
			@Override
			public int compare(Elite arg0, Elite arg1) {
				if(arg0.getScore() > arg1.getScore())
					return -1;
				if(arg0.getScore() < arg1.getScore())
					return 1;

				return Bytez.compareTo(arg0.getRow(),arg1.getRow());
			}
		});

		while(elites.size() > maxCount){
			elites.remove(elites.size()-1);
		}
	}

	public static void sortWithTags(Table hostTable, List<Elite> elites, List<Tag> tags, int maxCount){
		Map<byte[], Elite> map = new TreeMap<byte[], Elite>(Bytes.BYTES_COMPARATOR);

		List<byte[]> rowBytes = new Vector<byte[]>();
		for(Elite v : elites){
			rowBytes.add(v.getRow());
			map.put(v.getRow(), v);
		}
		rowBytes = new Recommender(hostTable, rowBytes).similar(tags).top(maxCount);

		elites.clear();
		for(byte[] row : rowBytes){
			elites.add(map.get(row));
		}
	}


	public static  Hashtable<String, Integer> replaceContent(String oldStr, String newStr) {
		try{
			Hashtable<String, Integer> news = Fenci.text2words(newStr);
			Hashtable<String, Integer> olds = Fenci.text2words(oldStr);

			return subWords(news, olds);
		}catch(Exception e){
			e.printStackTrace();
			return new Hashtable<String, Integer>();
		}
	}

	public static  Hashtable<String, Integer> subWords(Hashtable<String, Integer> news, Hashtable<String, Integer> olds){
		for(String word : olds.keySet()){
			Integer value = news.getOrDefault(word, 0);
			int count = value - olds.get(word);
			if(count == 0){
				news.remove(word);
			}else{
				news.put(word, count);
			}
		}
		return news;
	}
	public static  Hashtable<String, Integer> addWords(Hashtable<String, Integer> news, Hashtable<String, Integer> olds){
		for(String word : olds.keySet()){
			Integer value = news.getOrDefault(word, 0);
			int count = value + olds.get(word);
			if(count == 0){
				news.remove(word);
			}else{
				news.put(word, count);
			}
		}
		return news;
	}
	public static Hashtable<String, Integer> mulWords(Hashtable<String, Integer> olds, int factor){

		for(String word : olds.keySet()){
			olds.put(word, olds.get(word)*factor);
		}
		return olds;
	}

	public static Hashtable<String, Integer> updateTextIndex(SearchableEntity doc, String oldText, String newText, int weight){
		Hashtable<String, Integer> dif = DocUtil.replaceContent(oldText, newText);
		dif = DocUtil.mulWords(dif, weight);
		try {
			DocUtil.adjustDocWords(doc, "word", dif);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return dif;
	}


	public static void adjustDocWords(SearchableEntity doc, String column, Hashtable<String, Integer> fix) throws Exception{
		int wordCount = 0;
		int count = fix.size();

		List<String> words = new Vector<String>();
		words.addAll(fix.keySet());

		Idc idc= Idc.getInstance(doc.getBasisTable(), column);
		idc.connect();

//		Lunnar lunnar = new Lunnar(5);
//		lunnar.submit(new Runnable(){
//			@Override
//			public void run() {
//				Result[] results = BSL.getIdxTable().get(idxGets);
//				int newIdxCount = 0;
//				for(Result r : results){
//					if(r.isEmpty())newIdxCount ++ ;
//				}
//			}
//		});

		WordInfo[] wordArray = new WordInfo[count];

		//1. 整理
		List<Get> idxGets = new Vector<Get>();
		List<Get> indexGets = new Vector<Get>();
		for(int i=0; i<words.size(); i++){
			String word =  words.get(i);
			WordInfo wi = new WordInfo();
//			wi.word = word;
			wi.count = fix.get(word);
			wi.ref = 0;

			wordArray[i] = wi;
			wordCount += fix.get(word);

			byte[] ranking = Value.from(word).ranking();
			byte[] idxRow = Bytez.add(Bytez.from(idc.getId()), ranking);
			byte[] indexRow = Bytez.add(Bytez.from(idc.getId()), ranking, doc.getRow());

			wi.idxRow = idxRow;
			wi.indexRow = indexRow;

			idxGets.add(new Get(idxRow));
			indexGets.add(new Get(indexRow));
		}

		wordCount += doc.getWordCount();

		//2. 读取
		Result[] idxResults = Obase.getIdxTable().get(idxGets);
		Result[] indexResults = Obase.getIndexTable().get(indexGets);

		//3. 处理
		List<Put> idxPuts = new Vector<Put>();
		List<Put> indexPuts = new Vector<Put>();
		List<Delete> idxDeletes = new Vector<Delete>();
		List<Delete> indexDeletes = new Vector<Delete>();

		for(int i=0; i<count; i++){
			WordInfo wi = wordArray[i];

			//1 .处理idx
			Result x= idxResults[i];
			if(!x.isEmpty()){
				Idx idx = new Idx(x);
				wi.idx = idx;
				wi.ref = idx.getReference();
			}

			//2 .处理index
			Result r= indexResults[i];
			if(!r.isEmpty()){
				Index index = new Index(r);
//				wi.index = index;
				wi.count += Bytez.toInt(index.getData());

				if(wi.count == 0){
					wi.ref -= 1;
				}
			}else{
				wi.ref += 1;
			}

			//3. 更新
			//index delete
			//index update
			if(wi.count == 0){
				Delete del = new Delete(wi.indexRow);
				indexDeletes.add(del);
			}else{
				Put put = new Put(wi.indexRow);
				byte[] data = Bytes.add(Bytez.from(wi.count),Bytez.from(wordCount));
				put.addColumn(Bytez.from(Obase.FAMILY_ATTR), Bytez.from("data"), Index.toData(data) );
				indexPuts.add(put);
			}
			//idx delete
			//idx update
			if(wi.ref <= 0 && wi.idx != null){
				Delete del = new Delete(wi.idxRow);
				idxDeletes.add(del);
			}else if(wi.ref > 0){
				if(wi.idx!=null &&  wi.idx.getReference() == wi.ref){

				}else{
					Put put = new Put(wi.idxRow);
					put.addColumn(Bytez.from(Obase.FAMILY_ATTR), Bytez.from(Obase.COLUMN_REFERENCE), Bytez.from(wi.ref));
					idxPuts.add(put);
				}
			}
		}

		//4. 更新
		doc.setWordCount(wordCount);

		if(!indexPuts.isEmpty())
		Obase.getIndexTable().put(indexPuts);
		if(!indexDeletes.isEmpty())
		Obase.getIndexTable().delete(indexDeletes);

		if(!idxPuts.isEmpty())
		Obase.getIdxTable().put(idxPuts);
		if(!idxDeletes.isEmpty())
		Obase.getIdxTable().delete(idxDeletes);
	}

	private final static class WordInfo{
		long ref;
		Idx idx;
		byte[] idxRow;
		int count;
		byte[] indexRow;
	}
}
