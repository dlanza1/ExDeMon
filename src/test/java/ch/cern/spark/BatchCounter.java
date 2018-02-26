package ch.cern.spark;

import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.scheduler.StreamingListener;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchSubmitted;
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverError;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStopped;
import org.apache.spark.streaming.scheduler.StreamingListenerStreamingStarted;

/**
 * An object that counts the number of started / completed batches. This is
 * implemented using a StreamingListener. Constructing a new instance
 * automatically registers a StreamingListener on the given StreamingContext.
 */
public class BatchCounter implements StreamingListener{
	
	private int numCompletedBatches = 0;
	private int numStartedBatches = 0;
	private Time lastCompletedBatchTime;

	public BatchCounter() {
	}

	public int getNumCompletedBatches() {
		return numCompletedBatches;
	}

	public int getNumStartedBatches() {
		return numStartedBatches;
	}

	public Time getLastCompletedBatchTime() {
		return lastCompletedBatchTime;
	}

	@Override
	public void onBatchStarted(StreamingListenerBatchStarted listener) {
		synchronized (this) {
			numStartedBatches += 1;
			this.notifyAll();
		}
	}

	@Override
	public void onBatchCompleted(StreamingListenerBatchCompleted listener) {
		synchronized (this) {
			numCompletedBatches += 1;
			lastCompletedBatchTime = listener.batchInfo().batchTime();
		}
	}

	@Override
	public void onBatchSubmitted(StreamingListenerBatchSubmitted arg0) {
	}

	@Override
	public void onOutputOperationCompleted(StreamingListenerOutputOperationCompleted arg0) {
	}

	@Override
	public void onOutputOperationStarted(StreamingListenerOutputOperationStarted arg0) {
	}

	@Override
	public void onReceiverError(StreamingListenerReceiverError arg0) {
	}

	@Override
	public void onReceiverStarted(StreamingListenerReceiverStarted arg0) {
	}

	@Override
	public void onReceiverStopped(StreamingListenerReceiverStopped arg0) {
	}

    @Override
    public void onStreamingStarted(StreamingListenerStreamingStarted arg0) {   
    }
	
}
