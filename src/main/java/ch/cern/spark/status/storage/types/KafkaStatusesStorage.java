package ch.cern.spark.status.storage.types;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import com.google.common.collect.Sets;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.ByteArray;
import ch.cern.spark.RDD;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.JSONStatusSerializer;
import ch.cern.spark.status.storage.JavaStatusSerializer;
import ch.cern.spark.status.storage.StatusSerializer;
import ch.cern.spark.status.storage.StatusesStorage;
import scala.Tuple2;

public class KafkaStatusesStorage extends StatusesStorage {

	private static final long serialVersionUID = 1194347587683707148L;
	
	private Map<String, Object> kafkaProducer;
	private Map<String, Object> kafkaParams = null;

	private String topic;
	
	private StatusSerializer serializer;

	private transient KafkaConsumer<Object, Object> consumer;
	
	public void config(Properties properties) throws ConfigurationException {
		kafkaProducer = getKafkaProducerParams(properties);
		kafkaParams = getKafkaConsumerParams(properties);
        
		topic = properties.getProperty("topic");
		
		consumer = new KafkaConsumer<Object, Object>(kafkaParams);
		consumer.subscribe(Sets.newHashSet(topic));
		consumer.poll(0);
		
		String serializationType = properties.getProperty("serialization", "json");
		switch (serializationType) {
		case "json":
			serializer = new JSONStatusSerializer();
			break;
		case "java":
			serializer = new JavaStatusSerializer();
			break;
		default:
			throw new ConfigurationException("Serialization type " + serializationType + " is not available.");
		}
        
		properties.confirmAllPropertiesUsed();
	}
	
	@Override
	public JavaRDD<Tuple2<StatusKey, StatusValue>> load(JavaSparkContext context) throws IOException, ConfigurationException {

		OffsetRange[] offsetRanges = getTopicOffsets();
		
		JavaRDD<ConsumerRecord<Bytes, Bytes>> kafkaContent = 
				KafkaUtils.<Bytes, Bytes>createRDD(context, kafkaParams, offsetRanges, LocationStrategies.PreferConsistent());
		
		JavaRDD<Tuple2<ByteArray, ByteArray>> latestRecords = getLatestRecords(kafkaContent);
		
		return parseRecords(latestRecords);
	}
	
	private JavaRDD<Tuple2<ByteArray, ByteArray>> getLatestRecords(JavaRDD<ConsumerRecord<Bytes, Bytes>> kafkaContent) {
		return kafkaContent.mapToPair(consumedRecord -> {
									Bytes key = consumedRecord.key();
									
									Tuple2<Long, ByteArray> value = new Tuple2<>(consumedRecord.offset(), new ByteArray(consumedRecord.value().get()));
									
									return new Tuple2<ByteArray, Tuple2<Long, ByteArray>>(new ByteArray(key.get()), value);
								})
								.groupByKey()
								.map(pair -> {
									ByteArray key = pair._1;
									
									Iterator<Tuple2<Long, ByteArray>> values = pair._2.iterator();
									
									Tuple2<Long, ByteArray> latestValue = values.next();
									
									while(values.hasNext()) {
										Tuple2<Long, ByteArray> value = values.next();
										
										if(value._1 > latestValue._1)
											latestValue = value;
									}
									
									return new Tuple2<ByteArray, ByteArray>(key, latestValue._2);
								});
	}
	
	private JavaRDD<Tuple2<StatusKey, StatusValue>> parseRecords(JavaRDD<Tuple2<ByteArray, ByteArray>> latestRecords) {
		return latestRecords.map(binaryRecord -> new Tuple2<>(
														serializer.toKey(binaryRecord._1.get()),
														serializer.toValue(binaryRecord._2.get()))
													);
	}

	public OffsetRange[] getTopicOffsets() {
		Map<TopicPartition, OffsetRange> offsetRanges = new HashMap<>();	
		
		List<TopicPartition> partitions = new LinkedList<>();
		for (PartitionInfo partitionInfo : consumer.partitionsFor(topic))
			partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));

		long[] from = new long[partitions.size()];
		long[] until = new long[partitions.size()];
		
		consumer.seekToBeginning(partitions);
		for (TopicPartition tp : partitions)
			from[tp.partition()] = consumer.position(tp);
		
		consumer.seekToEnd(partitions);
		for (TopicPartition tp : partitions)
			until[tp.partition()] = consumer.position(tp);
		
		for (TopicPartition tp : partitions)
			offsetRanges.put(tp, OffsetRange.create(tp, from[tp.partition()], until[tp.partition()]));
		
		return offsetRanges.values().toArray(new OffsetRange[0]);
	}
	
	@Override
	public <K extends StatusKey, V extends StatusValue> void save(RDD<Tuple2<K, V>> rdd, Time time)
			throws IllegalArgumentException, IOException, ConfigurationException {
		
		rdd = filterOnlyUpdatedStates(rdd, time);
		
		rdd.asJavaRDD().foreachPartition(new KafkaProducerFunc<K, V>(kafkaProducer, serializer, topic));
	}
	
    private <K extends StatusKey, V extends StatusValue> RDD<Tuple2<K, V>> filterOnlyUpdatedStates(RDD<Tuple2<K, V>> rdd, Time time) {
		return RDD.from(rdd.asJavaRDD().filter(tuple -> tuple._2.getUpdatedTime() == time.milliseconds() || tuple._2.getUpdatedTime() == 0));
	}

	private Map<String, Object> getKafkaProducerParams(Properties props) {
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        
        kafkaParams.put("key.serializer", BytesSerializer.class);
        kafkaParams.put("value.serializer", BytesSerializer.class);
        
        Properties kafkaPropertiesFromConf = props.getSubset("producer");
        for (Entry<Object, Object> kafkaPropertyFromConf : kafkaPropertiesFromConf.entrySet()) {
            String key = (String) kafkaPropertyFromConf.getKey();
            String value = (String) kafkaPropertyFromConf.getValue();
            
            kafkaParams.put(key, value);
        }
        
        return kafkaParams;
    }
	
    private Map<String, Object> getKafkaConsumerParams(Properties props) {
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        
        kafkaParams.put("key.deserializer", BytesDeserializer.class);
        kafkaParams.put("value.deserializer", BytesDeserializer.class);
        
        Properties kafkaPropertiesFromConf = props.getSubset("consumer");
        for (Entry<Object, Object> kafkaPropertyFromConf : kafkaPropertiesFromConf.entrySet()) {
            String key = (String) kafkaPropertyFromConf.getKey();
            String value = (String) kafkaPropertyFromConf.getValue();
            
            kafkaParams.put(key, value);
        }
        
        return kafkaParams;
    }
    
    private static class KafkaProducerFunc<K extends StatusKey, V extends StatusValue> implements VoidFunction<Iterator<Tuple2<K, V>>>{

		private static final long serialVersionUID = 3712180876662835316L;
		
		private Map<String, Object> props;

		private String topic;

		private StatusSerializer serializer;
		
		protected KafkaProducerFunc(Map<String, Object> props, StatusSerializer serializer, String topic) {
			this.props = props;
			this.topic = topic;
			this.serializer = serializer;
		}

		@Override
		public void call(Iterator<Tuple2<K, V>> records) throws Exception {
			KafkaProducer<Bytes, Bytes> producer = new KafkaProducer<>(props);
			
			while(records.hasNext()) {
				Tuple2<K, V> record = records.next();
				
				Bytes key = new Bytes(serializer.fromKey(record._1));
				Bytes value = new Bytes(serializer.fromValue(record._2));
				
				producer.send(new ProducerRecord<Bytes, Bytes>(topic, key, value));
			}
			
			producer.close();
		}
    	
    }

}
