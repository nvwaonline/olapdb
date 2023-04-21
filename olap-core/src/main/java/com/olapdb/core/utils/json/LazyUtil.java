package com.olapdb.core.utils.json;

import java.util.Iterator;
import java.util.List;

public class LazyUtil {
    private char[] chars;
    private Iterator<Integer> iterator;
    private int left;
    private int pos;
    private StringBuilder sb = new StringBuilder("[");
    private int start;
    private int end;

    private LazyUtil(String text, List<Integer> lefts) {
        this.chars = text.toCharArray();
        this.iterator = lefts.iterator();
        this.left = iterator.next();
        this.pos = 0;
        this.start = 0;
        this.end = 0;
    }

    public final static String reduceJsonArray(String text, List<Integer> lefts) throws Exception {
        if(lefts.isEmpty())return "[]";

        LazyState stat = LazyState.UNKNOWN;
        LazyUtil util = new LazyUtil(text, lefts);

        int skip = 0;
        int length = text.length();
        for (int i = 0; i < length; i++) {
            char value = util.chars[i];

            switch (value) {
                case '\\':
                    skip += 1;
                    continue;
                case '[':
                    if (stat == LazyState.SCAN) {
                        throw new Exception("found '[' in LazyUtil");
                    }
                    if (stat == LazyState.UNKNOWN && i == 0) {
                        //create node
                        stat = LazyState.SCAN;
                        util.start = i + 1;
                    }
                    break;
                case ']':
                    if (i != length - 1) {
                        break;
                    }
                    switch (stat) {
                        case SCAN_NORMAL:
                        case SCAN_QUOTO_END:
                            //submit node
                            util.end = i;
                            util.pushNode();
                            stat = LazyState.END;
                            break;
                    }
                    break;
                case '"':
                    if (skip % 2 == 1) {
                        continue;
                    }
                    switch (stat) {
                        case SCAN:
                            //create node
                            stat = LazyState.SCAN_QUOTO;
                            break;
                        case SCAN_QUOTO:
                            stat = LazyState.SCAN_QUOTO_END;
                            util.end = i;
                            break;
                    }
                    break;
                case ',':
                    switch (stat) {
                        case SCAN_NORMAL:
                        case SCAN_QUOTO_END:
                            //submit node
                            util.end = i;
                            util.pushNode();
                            //create node
                            stat = LazyState.SCAN;
                            util.start = i;
                            break;
                    }
                    break;
                case '{':
                    if (stat == LazyState.SCAN) {
                        throw new Exception("found '{' in LazyUtil");
                    }
                    break;
                default:
                    if (stat == LazyState.SCAN) {
                        stat = LazyState.SCAN_NORMAL;
                    }
                    break;
            }

            skip = 0;
        }

        if (stat != LazyState.END) {
            return null;
        }

        if (util.left != Integer.MAX_VALUE) {
            throw new Exception("dimension not match!");
        }

        util.sb.append("]");
        return util.sb.toString();
    }

    private void pushNode() {
        if (pos == left) {
            if (sb.length() == 1 && chars[start] == ',') {
                sb.append(chars, start + 1, end - start - 1);
            } else if (sb.length() > 1 && chars[start] != ',') {
                sb.append(',');
                sb.append(chars, start, end - start);
            } else {
                sb.append(chars, start, end - start);
            }

            if (iterator.hasNext()) {
                left = iterator.next();
            } else {
                left = Integer.MAX_VALUE;
            }
        }

        pos += 1;
    }
}
