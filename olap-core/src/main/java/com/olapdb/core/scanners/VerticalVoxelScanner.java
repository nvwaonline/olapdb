package com.olapdb.core.scanners;

import com.olapdb.core.domain.MultiMeasureStat;
import com.olapdb.core.hll.HLLDistinct;
import com.olapdb.core.tables.Voxel;
import com.olapdb.obase.data.Bytez;
import com.olapdb.obase.data.index.Lunnar;
import com.olapdb.obase.utils.Obase;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class VerticalVoxelScanner {
    // special marker to indicate when a scanning task has finished
    private static final Voxel MARKER = new Voxel("marker", false);

    // number of scanning tasks still running
    private AtomicInteger taskCount = new AtomicInteger(0);

    // the size limited buffer to use
    private BlockingDeque<Voxel> results;

    private volatile Exception exception;

    private long destSegId; //输出的Voxel的Seg ID
    private boolean cacheBlocks;
    private boolean inclusiveStart = true;
    private boolean checkMatch = false;

    private AtomicLong scanCounter = new AtomicLong();

    private TreeSet<IndicatorScanner> scanners = new TreeSet<>(new Comparator<IndicatorScanner>() {
        @Override
        public int compare(IndicatorScanner o1, IndicatorScanner o2) {
            int result = Bytez.compareTo(o1.indicator, o2.indicator);

            if(result == 0){
                result = o1.scanner.hashCode() - o2.scanner.hashCode();
            }

            return result;
        }
    });

    private Thread loadingThread = new Thread(){
        public void run(){
        List<Voxel> currents =  new ArrayList<>();

        try {
            while(taskCount.get() > 0){
                IndicatorScanner scanner = scanners.pollFirst();
                Voxel voxel = scanner.voxel;

                if(!currents.isEmpty() && Bytez.compareTo(currents.get(0).getIndicator(), voxel.getIndicator()) !=0){
                    Voxel cbResult = combine(currents);
                    if(cbResult.valid()) {
                        results.put(cbResult);
                    }
                    currents.clear();
                }
                currents.add(voxel);

                if(!addScannerToTree(scanner)){
                    taskCount.decrementAndGet();
                }

                scanCounter.incrementAndGet();
            }

            if(! currents.isEmpty()){
                if(results != null) {
                    Voxel cbResult = combine(currents);
                    if(cbResult.valid()) {
                        results.put(cbResult);
                    }
                }
            }

            if(results != null) {
                results.put(MARKER);
            }
        }catch (Exception e){
            exception = e;
        }
        }
    };

    private Voxel combine(List<Voxel> voxels){
        Voxel first = voxels.get(0);
        Voxel result = new Voxel(destSegId,first.getIndicator(), false);
        if(voxels.size() == 1){
            byte[] bytes = first.getAttribute("stat");
            if(bytes != null){
                result.setAttribute("stat", bytes);
            }

            bytes = first.getAttribute("distinct");
            if(bytes != null){
                result.setAttribute("distinct", bytes);
            }
        }else{
            MultiMeasureStat stat = first.getStat();
            for(int i=1; i<voxels.size();i++){
                stat.combine(voxels.get(i).getStat());
            }
            result.setStat(stat);

            HLLDistinct distinct = first.getDistinct();
            if(distinct != null){
                for(int i=1; i<voxels.size();i++){
                    distinct.merge(voxels.get(i).getDistinct());
                }
                result.setDistinct(distinct);
            }
        }

        return result;
    }

    public VerticalVoxelScanner( List<Long> segIds, long destSegId, int cuboidId, Filter filter) throws Exception {
        this(segIds, destSegId, Bytez.from(cuboidId), Bytez.next(Bytez.from(cuboidId)), filter, true, true, false);
    }

    public VerticalVoxelScanner(List<Long> segIds, long destSegId, byte[] startValue, byte[] stopValue, Filter filter, boolean cacheBlocks, boolean inclusiveStart, boolean checkMatch) throws Exception {
        this.destSegId = destSegId;
        this.cacheBlocks = cacheBlocks;
        this.inclusiveStart = inclusiveStart;
        this.checkMatch = checkMatch;
        init(segIds, startValue, stopValue, filter, 10000);
    }

    private void init(List<Long> segIds, byte[] startValue, byte[] stopValue, Filter filter, int bufferSize) throws Exception {
        bufferSize = bufferSize>0?bufferSize:1000;
        results = new LinkedBlockingDeque<>(bufferSize);
        taskCount.set(0);

        final AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
        Lunnar lunnar = new Lunnar(Math.min(100, segIds.size()));
        for(long seg : segIds){
            lunnar.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        byte[] start = Bytez.add(Bytez.from(seg), startValue);
                        byte[] stop = Bytez.add(Bytez.from(seg), stopValue);
                        Scan scan = new Scan().withStartRow(start, inclusiveStart).withStopRow(stop).setCaching(1000);
                        scan.setCacheBlocks(VerticalVoxelScanner.this.cacheBlocks);
                        if(filter != null) {
                            scan.setFilter(filter);
                        }

                        ResultScanner rs = Obase.getTable(Voxel.class).getScanner(scan);
                        if(VerticalVoxelScanner.this.addScannerToTree(new IndicatorScanner(rs))) {
                            taskCount.incrementAndGet();
                        }
                    }catch (Exception e){
                        log.error("create partion scan failed", e);
                        exceptionAtomicReference.set(e);
                    }
                }
            });
        }

        lunnar.waitForComplete();

        if(this.checkMatch && this.scanners.size() != segIds.size()){
            throw new Exception("FlOOD ERROR: create vertical voxel scanner failed. segIds.size = " + segIds.size() + " not match scanners.size = "+ this.scanners.size() );
        }

        if(exceptionAtomicReference.get() != null){
            throw exceptionAtomicReference.get();
        }

        loadingThread.start();
    }

    public long getScanCount(){
        return scanCounter.get();
    }

    public Voxel next() throws Exception {
        try {
            // if at least one task is active wait for results to arrive.
            if (exception != null) {
                throw exception;
            }

            Voxel r = results.take();
            // skip markers, adjust task count if needed
            if (r != MARKER) {
                return r;
            }

            return results.poll();
        } catch (InterruptedException x) {
            close();
            throw new IOException(x);
        }
    }

    public List<Voxel> next(int nbRows) throws Exception {
        List<Voxel> resultSets = new ArrayList(nbRows);

        for(int i = 0; i < nbRows; ++i) {
            Voxel next = this.next();
            if (next == null) {
                break;
            }

            resultSets.add(next);
        }

        return resultSets;
    }

    public void close() {
        results = null;

        if(loadingThread!=null && loadingThread.isAlive()) {
            loadingThread.interrupt();
        }
        loadingThread = null;

        scanners.forEach(e->e.scanner.close());
    }

    synchronized private boolean addScannerToTree(IndicatorScanner indicatorScanner)throws Exception{
        Result result = indicatorScanner.scanner.next();
        if(result == null){
            indicatorScanner.scanner.close();
            return false;
        }

        Voxel voxel = new Voxel(result);
        indicatorScanner.voxel = voxel;
        indicatorScanner.indicator = voxel.getIndicator();
        this.scanners.add(indicatorScanner);

        return true;
    }

    private final static class IndicatorScanner{
        byte[] indicator;
        ResultScanner scanner;
        Voxel voxel;

        IndicatorScanner(ResultScanner scanner){
            this.scanner = scanner;
        }
    }
}
