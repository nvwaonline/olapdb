package com.olapdb.core.utils;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Slf4j
public class TimeUtil {
    public final static List<String> OlapTimeDims = Arrays.asList("OLAP_YEAR", "OLAP_MONTH", "OLAP_DAY", "OLAP_HOUR", "OLAP_MINUTE", "OLAP_SECOND", "OLAP_MILLISECOND");
    private final static List<String> OlapTimeDefault = Arrays.asList("0000", "00", "00", "00", "00", "00", "000");
    private final static SimpleDateFormat SimpleDateFormat23 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final static SimpleDateFormat simpleDateFormat23 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final static SimpleDateFormat simpleDateFormat19 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private final static List<String> OlapTimeSeperate = Arrays.asList("-", "-", " ", ":", ":", ".", "");

    /**
     * 得到7段Text时间
     * @param date
     * @return
     */
    public static List<String> time2segs(Date date)
    {
        List<String> result = new Vector<>(7);
        String dateStr = null;
        synchronized (SimpleDateFormat23) {
            dateStr = SimpleDateFormat23.format(date);
        }

        result.add(dateStr.substring(0,4));
        result.add(dateStr.substring(5,7));
        result.add(dateStr.substring(8,10));
        result.add(dateStr.substring(11,13));
        result.add(dateStr.substring(14,16));
        result.add(dateStr.substring(17,19));
        result.add(dateStr.substring(20,23));

        return result;
    }

    /**
     * 把一个json数据中的时间维度用层级时间维度组替换
     * @param jo
     * @param timeDim
     * @param timeDimType
     * @return
     */
    public static JSONObject replaceTimeDimensionToTimeHierachy(JSONObject jo, String timeDim, String timeDimType, AtomicLong youngestDataTime, AtomicLong eldestDataTime){
        String timeInfo = (String)jo.remove(timeDim);
        if(timeInfo != null && !timeInfo.isEmpty()){
            try{
                Date date = null;
                if(timeInfo.length() == 23) {
                    synchronized (simpleDateFormat23) {
                        date = simpleDateFormat23.parse(timeInfo);
                    }
                }
                else {
                    synchronized (simpleDateFormat19) {
                        date = simpleDateFormat19.parse(timeInfo);
                    }
                }
                synchronized (youngestDataTime) {
                    if (date.getTime() > youngestDataTime.get()) {
                        youngestDataTime.set(date.getTime());
                    }
                    if (date.getTime() < eldestDataTime.get()) {
                        eldestDataTime.set(date.getTime());
                    }
                }
                List<String> timeSegs = time2segs(date);
                for(int i=0; i<OlapTimeDims.size(); i++){
                    jo.put(OlapTimeDims.get(i), timeSegs.get(i));
                }
            }catch (Exception e){
                log.error("replaceTimeDimensionToTimeHierachy error fact={} timeDim={} timeInfo={}", jo.toJSONString(), timeDim, timeInfo, e);
            }
        }else{
            for(int i=0; i<OlapTimeDims.size(); i++){
                jo.put(OlapTimeDims.get(i), OlapTimeDefault.get(i));
            }
        }
        return jo;
    }

    /**
     * a:b:c ...  替换其中的时间维度为时间层级维度
     * @param group
     * @param timeDim
     * @return
     */
    public static String replaceTimeDimension(String group, String timeDim){
        List<String> dims = Arrays.asList(group.split(":")).stream().map(e->e.equals(timeDim)?StringUtils.join(OlapTimeDims, ':'):e).collect(Collectors.toList());
        return StringUtils.join(dims, ':');
    }

    /**
     * a:b:c ...  替换其中的时间维度为时间层级维度
     * @param group
     * @param timeDim
     * @return
     */
    public static void replaceTimeDimension(List<String> group, String timeDim){
        if(group.remove(timeDim)){
            group.addAll(OlapTimeDims);
        }
    }

    private static String reduceTimeString(String timeStr){
        int length = timeStr.length();
        boolean stop = false;
        while(length > 0 && !stop){
            switch (timeStr.charAt(length-1)){
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9':
                    stop = true;
                    break;
                case '0':
                default:
                    length -= 1;
                    continue;
            }
        }

        return timeStr.substring(0, length);
    }
    /**
     * 根据时间条件时分离出需要的时间维度
     * @param timeConditions
     */
    public static List<String> deriveTimeDimension(List<String> timeConditions){
        OptionalInt find = timeConditions.stream().mapToInt(e->reduceTimeString(e).length()).max();
        int need = 7;
        if(find.isPresent()){
            switch (find.getAsInt()){
                case 0:
                case 1:
                case 2:
                case 3:
                    need = 0;
                    break;
                case 4:
                    need = 1;
                    break;
                case 5:
                case 6:
                case 7:
                    need = 2;
                    break;
                case 8:
                case 9:
                case 10:
                    need = 3;
                    break;
                case 11:
                case 12:
                case 13:
                    need = 4;
                    break;
                case 14:
                case 15:
                case 16:
                    need = 5;
                    break;
                case 17:
                case 18:
                case 19:
                    need = 6;
                    break;
                case 20:
                case 21:
                case 22:
                case 23:
                    need = 7;
                default:
                    break;
            }
        }

        return OlapTimeDims.subList(0,need);
    }

    /**
     * 计算时间维度的深度
     * @param dims
     * @return
     */
    public static int calcTimeHierachyDepth(List<String> dims){
        int depth = 0;
        for(String dim : OlapTimeDims){
            if(dims.contains(dim))
                depth+= 1;
            else
                break;;
        }

        return depth;
    }

    /**
     * 构建olap时间维过滤扫描前缀
     * @param timeExpression
     * @param timeHierachyDepth
     * @return
     */
    public static String buildTimeFilterPrefix(String timeExpression, int timeHierachyDepth){
        if(timeHierachyDepth <= 0)return null;

        String expression = "[";
        if(timeHierachyDepth >= 1){
            expression += "\"" + timeExpression.substring(0,4) +  "\"";
            if(timeHierachyDepth != 1)expression += ",";
        }
        if(timeHierachyDepth >= 2){
            expression += "\"" + timeExpression.substring(5,7) +  "\"";
            if(timeHierachyDepth != 2)expression += ",";
        }
        if(timeHierachyDepth >= 3){
            expression += "\"" + timeExpression.substring(8,10) +  "\"";
            if(timeHierachyDepth != 3)expression += ",";
        }
        if(timeHierachyDepth >= 4){
            expression += "\"" + timeExpression.substring(11,13) +  "\"";
            if(timeHierachyDepth != 4)expression += ",";
        }
        if(timeHierachyDepth >= 5){
            expression += "\"" + timeExpression.substring(14,16) +  "\"";
            if(timeHierachyDepth != 5)expression += ",";
        }
        if(timeHierachyDepth >= 6){
            expression += "\"" + timeExpression.substring(17,19) +  "\"";
            if(timeHierachyDepth != 6)expression += ",";
        }
        if(timeHierachyDepth == 7){
            if(timeExpression.length() >= 23)
            expression += "\"" + timeExpression.substring(20,23) +  "\"";
            else
                expression += "\"000\"";
        }

        return expression;
    }


    /**
     * 合并分离的层级时间维度
     * @param dimsToValueMap
     * @param timeDim
     */
    public static void combineTimeHierachy(Map<String, String> dimsToValueMap, String timeDim){
        /**
         * 不包含时间维度
         */
        if(!dimsToValueMap.containsKey(OlapTimeDims.get(0))){
            return;
        }

        StringBuffer sb = new StringBuffer();
        for(int i=0; i<OlapTimeDims.size(); i++){
//            String value = dimsToValueMap.remove(OlapTimeDims.get(i));
            String value = dimsToValueMap.get(OlapTimeDims.get(i));
            if(value == null){
                value = OlapTimeDefault.get(i);
            }

            sb.append(value + OlapTimeSeperate.get(i));
        }
        dimsToValueMap.put(timeDim, sb.toString());
    }


    public static void main(String[] args){
        System.out.println(time2segs(new Date(System.currentTimeMillis())));
    }
}
