package com.olapdb.core.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public class CombinationUtil {
    public static List<List<String>> combiantion(String[] items){
        List<List<String>> results = new Vector<>();

        if(items==null||items.length==0){
            return results;
        }
        List<String> list=new ArrayList<>();
        for(int i=0; i<=items.length; i++){
            combine(items,0, i, list, results);
        }

        return results;
    }

    public static void combine(String[] items, int begin, int number, List<String> list, List<List<String>> results){
        if(number==0){
            List<String> result = new ArrayList<>();
            result.addAll(list);
            results.add(result);
            return ;
        }
        if(begin==items.length){
            return;
        }
        list.add(items[begin]);
        combine(items,begin+1,number-1, list, results);
        list.remove(items[begin]);
        combine(items,begin+1, number, list, results);
    }

    public static void main(String args[]){
        String[] chs = {"a","b","c","d","e"};
        List<List<String>> results = combiantion(chs);
        System.out.println("total = " + results.size());

        for(List<String> result : results){
            System.out.println(result.toString());
        }
    }
}
