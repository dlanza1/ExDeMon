package ch.cern.spark.metrics.store;

import java.io.Serializable;

/**
 * Data contained here will be serialized and may thousands of these are stored.
 * Taking into account that, it should contain as less data as possible.
 *
 */
public interface Store extends Serializable{

}
