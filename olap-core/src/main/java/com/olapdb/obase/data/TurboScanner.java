package com.olapdb.obase.data;

import com.olapdb.obase.utils.Obase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Implements the scanner interface for the HBase client.
 */
public class TurboScanner extends AbstractClientScanner {
    private final Log LOG = LogFactory.getLog(this.getClass());

    // special marker to indicate when a scanning task has finished
    private static final Result MARKER = new Result();
    // the size limited buffer to use
    private BlockingQueue<Result> results;

    private volatile IOException exception;

    private Thread thread ;
    private ResultScanner rs;
    private boolean scanFinished = false;

    /**
     * Create a new push scanner.
     *
     * @param scan A Scan object describing the scan
     * @throws IOException
     */
    public TurboScanner(Class entityClass, Scan scan, int bufferSize){
        results = new LinkedBlockingQueue<>(bufferSize);

        final Table table = Obase.getTable(entityClass);
        this.thread = new Thread(){
            public void run(){
                try {
                    TurboScanner.this.rs = table.getScanner(scan);
                    for(Result r: rs){
                        results.put(r);
                    }
                    results.put(MARKER);
                }catch(Exception x){
                    TurboScanner.this.exception = x instanceof IOException ? (IOException) x : new IOException(x);
                }
            }
        };

        this.thread.setName(table.getName().getNameAsString()+ ".turboScanner");
        this.thread.start();
    }

    @Override
    public Result next() throws IOException {
        try {
            // if at least one task is active wait for results to arrive.
            if (exception != null) {
                throw exception;
            }

            if(!scanFinished){
                Result r = results.take();
                // skip markers, adjust task count if needed
                if (r == MARKER) {
                    scanFinished = true;
                }else {
                    return r;
                }
            }

            return results.poll();
        } catch (InterruptedException x) {
            this.thread.interrupt();
            throw new IOException(x);
        }
    }

    @Override
    public void close() {
        results = null;
        if(thread.isAlive()) {
            thread.interrupt();
        }
        this.rs.close();
    }

    @Override
    public boolean renewLease(){
        return rs.renewLease();
    }
}
