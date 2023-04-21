package com.olapdb.core.utils.json;

import java.util.ArrayList;
import java.util.List;

public class LazyArray {
    private char[] chars;
    private List<LazyNode> nodes = new ArrayList<>();
    private LazyState stat = LazyState.UNKNOWN;
    private LazyArray(String text){
        this.chars = text.toCharArray();
    }

    public final static LazyArray parseArray(String text)throws Exception {
        LazyArray array = new LazyArray(text);
        LazyNode node = null;

        int skip = 0;
        int length = text.length();
        for(int i=0; i<length; i++){
            char value = array.chars[i];

            switch(value){
                case '\\':
                    skip += 1 ;
                    continue;
                case '[':
                    if(array.stat == LazyState.SCAN){
                        throw new Exception("found '[' in LazyArray");
                    }
                    if(array.stat == LazyState.UNKNOWN  && i==0){
                        //create node
                        array.stat = LazyState.SCAN;
                        node = new LazyNode();
                        node.start = i+1;
                    }
                    break;
                case ']':
                    if(i != length-1){
                        break;
                    }
                    switch (array.stat){
                        case SCAN_NORMAL:
                        case SCAN_QUOTO_END:
                            //submit node
                            node.end = i;
                            array.nodes.add(node);
                            array.stat = LazyState.END;
                            break;
                    }
                    break;
                case '"':
                    if(skip%2==1){
                        continue;
                    }
                    switch (array.stat){
                        case SCAN:
                            //create node
                            array.stat = LazyState.SCAN_QUOTO;
                            break;
                        case SCAN_QUOTO:
                            array.stat = LazyState.SCAN_QUOTO_END;
                            node.end = i;
                            break;
                    }
                    break;
                case ',':
                    switch (array.stat){
                        case SCAN_NORMAL:
                        case SCAN_QUOTO_END:
                            //submit node
                            node.end = i;
                            array.nodes.add(node);
                            //create node
                            array.stat = LazyState.SCAN;
                            node = new LazyNode();
                            node.start = i;
                            break;
                    }
                    break;
                case '{':
                    if(array.stat == LazyState.SCAN){
                        throw new Exception("found '{' in LazyArray");
                    }
                    break;
                default:
                    if(array.stat == LazyState.SCAN){
                        array.stat = LazyState.SCAN_NORMAL;
                    }
                    break;
            }

            skip = 0;
        }

        if(array.stat != LazyState.END){
            return null;
        }

        return array;
    }

    public String toJSONString() {
        StringBuilder sb = new StringBuilder("[");
        for(LazyNode node : nodes){
            if(sb.length() ==1 && chars[node.start] == ',') {
                sb.append(chars, node.start+1, node.end - node.start-1);
            }else if(sb.length() >1 && chars[node.start] != ','){
                sb.append(',');
                sb.append(chars, node.start, node.end - node.start);
            }
            else{
                sb.append(chars, node.start, node.end - node.start);
            }
        }
        sb.append("]");
        return sb.toString();
    }

    public String toJSONString(List<Integer> lefts) {
        StringBuilder sb = new StringBuilder("[");
        for(int i : lefts){
            LazyNode node = nodes.get(i);
            if(sb.length() ==1 && chars[node.start] == ',') {
                sb.append(chars, node.start+1, node.end - node.start-1);
            }else if(sb.length() >1 && chars[node.start] != ','){
                sb.append(',');
                sb.append(chars, node.start, node.end - node.start);
            }
            else{
                sb.append(chars, node.start, node.end - node.start);
            }
        }
        sb.append("]");
        return sb.toString();
    }

    public LazyNode removeNode(int index){
        return nodes.remove(index);
    }
    public void reduce(List<Integer> lefts){
        List<LazyNode> leftNodes = new ArrayList<>(lefts.size());
        for(int i: lefts){ leftNodes.add(nodes.get(i)); }
        nodes = leftNodes;
    }

    public int size(){
        return nodes.size();
    }
}
