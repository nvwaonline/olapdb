package com.olapdb.core.scanners;

import com.olapdb.core.tables.Fact;
import com.olapdb.obase.data.Bytez;
import com.olapdb.obase.data.index.Lunnar;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class HorizontalFactScanner implements Iterator<Fact> {
    private TreeSet<IndicatorFactScanner> scanners = new TreeSet<>(new Comparator<IndicatorFactScanner>() {
        @Override
        public int compare(IndicatorFactScanner o1, IndicatorFactScanner o2) {
            int result = Bytez.compareTo(o1.indicator, o2.indicator);

            if(result == 0){
                result = o1.scanner.hashCode() - o2.scanner.hashCode();
            }

            return result;
        }
    });


    public HorizontalFactScanner(List<ResultScanner> resultScanners) throws Exception {
        init( resultScanners);
    }

    private void init(List<ResultScanner> resultScanners) throws Exception {
//        for(ResultScanner turboScanner : resultScanners){
//            this.addScannerToTree(new IndicatorFactScanner(turboScanner));
//        }

        AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();

        Lunnar lunnar = new Lunnar();
        for(ResultScanner turboScanner : resultScanners){
            final ResultScanner scanner = turboScanner;
            lunnar.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        HorizontalFactScanner.this.addScannerToTree(new IndicatorFactScanner(scanner));
                    }catch (Exception e){
                        exceptionAtomicReference.set(e);
                    }
                }
            });
        }
        lunnar.waitForComplete();

        if(exceptionAtomicReference.get() != null){
            throw exceptionAtomicReference.get();
        }

        log.info("Scanner size = {}", scanners.size());
    }

    private Fact current = null;
    @Override
    public boolean hasNext() {
        if(current != null){
            return true;
        }

        try {
            current = moveNext();
        }catch (Exception e){
            e.printStackTrace();
        }

        return current != null;
    }

    public Fact next()
    {
        if (!hasNext()) {
            return null;
        }

        Fact temp = current;
        current = null;
        return temp;
    }

    private Fact moveNext() throws Exception {
        if(scanners.isEmpty())return null;

        IndicatorFactScanner scanner = scanners.pollFirst();

        Fact fact = scanner.fact;
        addScannerToTree(scanner);

        return fact;
    }

    public void close() {
        log.info("Scan pool closed.");

        for(IndicatorFactScanner scanner : scanners){
            scanner.scanner.close();
        }
    }

    synchronized private void addScannerToTree(IndicatorFactScanner indicatorScanner)throws Exception{
        Result result = indicatorScanner.scanner.next();
        if(result == null){
            indicatorScanner.scanner.close();
            return ;
        }
        Fact fact = new Fact(result);

        indicatorScanner.fact = fact;
        indicatorScanner.indicator = fact.getIndicator();
        this.scanners.add(indicatorScanner);
    }

    private final static class IndicatorFactScanner {
        byte[] indicator;
        ResultScanner scanner;
        Fact fact;

        IndicatorFactScanner(ResultScanner scanner){
            this.scanner = scanner;
        }
    }
}
