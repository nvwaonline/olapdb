package com.olapdb.obase.data.index;

import java.util.concurrent.*;

public class Lunnar {
	private  ExecutorService executer = null;
	private  LinkedBlockingQueue<Runnable> queue = null;

	public Lunnar(){
		this(Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().availableProcessors());
	}
	public Lunnar(int size){ this(size, size);	}
	public Lunnar(int size, int queueSize){
		if(size > 500)size = 500;
		if(size<1)size = 1;
		if(queueSize < 1)queueSize = 1;

		queue = new LinkedBlockingQueue<Runnable>(queueSize);
		executer = new ThreadPoolExecutor(size, size,
				0L, TimeUnit.MILLISECONDS,
				queue,
				(r, executor) -> {
					try {
						executor.getQueue().put(r);
					} catch (InterruptedException e) {
						throw new RejectedExecutionException("interrupted", e);
					}
				});
	}


	public int getBlockingSize(){
		return queue.size();
	}

	public void submit(Runnable job){
		executer.submit(job);
	}

	public void waitForComplete(){
		executer.shutdown();

		try {
			executer.awaitTermination(10, TimeUnit.HOURS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void waitForComplete(long time, TimeUnit timeUnit){
		executer.shutdown();

		try {
			executer.awaitTermination(time, timeUnit);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void close(){
		if(!executer.isShutdown()) {
			executer.shutdownNow();
		}
	}
}
