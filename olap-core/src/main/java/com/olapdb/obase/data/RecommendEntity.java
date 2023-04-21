package com.olapdb.obase.data;

import com.olapdb.obase.utils.Annealing;
import com.olapdb.obase.utils.VectorUtil;
import org.apache.hadoop.hbase.client.Result;

import java.util.*;
import java.util.stream.Collectors;

public abstract class RecommendEntity extends SearchableEntity{
	public RecommendEntity(byte[] row){
		super(row);
	}

	public RecommendEntity(Result r) {
		super(r);
	}

	public double getAneCoef(){return Annealing.FIRM_COEF;}
	public int getTagDimension(){return 100;}

	public void learningTag(String tag, Double rate){
		byte[] data = this.getAttribute("tag", tag);
		double old = 0;
		if(data != null){
			old = Bytez.toDouble(data);
		}

		this.setAttribute("tag", tag, Bytez.from(Annealing.add(old, rate, this.getAneCoef())));
		checkTagDimension();
	}

	public void learningTags_Old(List<Tag> tags, Double rate){
		if(tags == null || tags.isEmpty() || rate ==0)
			return;

		int len = tags.size();
		String[] items = new String[len];
		byte[][] values = new byte[len][];

		for(int i=0; i<len; i++){
			Tag tag = tags.get(i);
			byte[] data = this.getAttribute("tag", tag.word);
			double old = 0;
			if(data != null){
				old = Bytez.toDouble(data);
			}

			double learning = tag.weight*rate;

			items[i] = tag.word;
			values[i] = Bytez.from(Annealing.add(old, learning, this.getAneCoef()));
		}

		this.setAttribute("tag", items, values);
		checkTagDimension();
	}

	public void learning(RecommendEntity other, double rate){
		learningTags(other.getNormalTags(), rate);
	}

	public void learningTags(List<Tag> tags, Double rate){
		if(tags == null || tags.isEmpty() || rate ==0)
			return;
		List<Tag> oldTags = this.getTags();

		Map<String, Double> olds = new HashMap<String, Double>();
		Map<String, Double> curs = new HashMap<String, Double>();
		for(Tag t : oldTags){
			olds.put(t.word, t.weight);
		}
		curs.putAll(olds);

		for(Tag t:tags){
			double old = curs.getOrDefault(t.word, new Double(0));
			double learning = t.weight*rate;
			curs.put(t.word, Annealing.add(old, learning, this.getAneCoef()));
		}
		List<Tag> curTags = new Vector<Tag>();
		for(String word : curs.keySet()){
			curTags.add(new Tag(word, curs.get(word)));
		}
		formatTags(curTags);
		curs.clear();
		for(Tag t:curTags){
			curs.put(t.word, t.weight);
		}

		//update
		List<String> updates = new Vector<String>();
		for(Tag t : tags){
			if(curs.containsKey(t.word)){
				updates.add(t.word);
			}
		}
		Collections.sort(updates);
		if(!updates.isEmpty()){
			int len = updates.size();
			String[] items = new String[len];
			byte[][] values = new byte[len][];

			for(int i=0; i<len; i++){
				items[i] = updates.get(i);
				values[i] = Bytez.from(curs.get(items[i]));
			}

			this.setAttribute("tag", items, values);
		}

		//remove
		List<String> removes = new Vector<String>();
		for(Tag t : oldTags){
			if(!curs.containsKey(t.word)){
				removes.add(t.word);
			}
		}
		if(!removes.isEmpty()){
			this.deleteAttributes("tag", removes);
		}
	}

	public void setTags(List<Tag> tags){
		if(tags == null || tags.isEmpty())
			return;

		formatTags(tags);

		int len = tags.size();
		String[] items = new String[len];
		byte[][] values = new byte[len][];

		for(int i=0; i<len; i++){
			Tag tag = tags.get(i);
			items[i] = tag.word;
			values[i] = Bytez.from(Annealing.add(0, tag.weight, this.getAneCoef()));
		}
		this.setAttribute("tag", items, values);
		checkTagDimension();
	}

	public List<Tag> getTags() {
		return getAttributeItems("tag").stream()
				.map(item->new Tag(item, Bytez.toDouble(this.getAttribute("tag", item))))
				.sorted().collect(Collectors.toList());
	}

	public List<Tag> getNormalTags() {
		List<Tag> tags = getTags();

		double total = tags.stream().map(t->t.weight*t.weight).reduce(0.0, (x,y)->x+y);
		if(total == 0)total = 1;

		final double length = Math.sqrt(total);;

		tags.forEach(t->t.weight/=length);

		return tags;
	}

	public List<Tag> getShowTags(int size){
		List<Tag> tags = this.getTags();
		Collections.sort(tags);

		tags = tags.subList(0, Math.min(size, tags.size()));
		for(Tag t : tags){
			t.weight = Annealing.anneValue(t.weight, this.getAneCoef());
		}

		return tags;
	}

	public List<Tag> getAnneTags(){
		List<Tag> tags = this.getTags();
		for(Tag t : tags){
			t.weight = Annealing.anneValue(t.weight, this.getAneCoef());
		}

		return tags;
	}

	public double similar(RecommendEntity other){
		return VectorUtil.calcSimilar(getTags(), other.getTags());
	}


	public static double calcSimilar(RecommendEntity a, RecommendEntity b){
		return VectorUtil.calcSimilar(a.getTags(), b.getTags());
	}

	private void formatTags(List<Tag> tags){
		Collections.sort(tags);

		for(int i=tags.size()-1; i > this.getTagDimension()-1; i--){
			tags.remove(i);
		}
	}

	private void checkTagDimension(){
		List<Tag> tags = this.getTags();
		if(tags.size() <= this.getTagDimension())
			return;

		Collections.sort(tags);
		List<String> removes = new Vector<String>();
		for(int i=this.getTagDimension(); i<tags.size(); i++ ){
			removes.add(tags.get(i).word);
		}

		this.deleteAttributes("tag", removes);
	}

}
