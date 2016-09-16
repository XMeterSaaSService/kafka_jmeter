package org.apache.jmeter.protocol.kafka.sampler;

import java.text.MessageFormat;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaSampler extends AbstractSampler implements TestStateListener {
	private static final long serialVersionUID = 1L;
	private static final String KAFKA_BROKERS = "kafka.brokers";
	private static final String KAFKA_TOPIC = "kafka.topic";
	//private static final String KAFKA_KEY = "kafka.key";
	private static final String KAFKA_MESSAGE = "kafka.message";
	private static final String KAFKA_MESSAGE_SERIALIZER = "kafka.message.serializer";
	private static final String KAFKA_KEY_SERIALIZER = "kafka.key.serializer";
	private static ConcurrentHashMap<String, Producer<String, String>> producers = new ConcurrentHashMap<>();
	private static final Logger log = LoggingManager.getLoggerForClass();
	
	public KafkaSampler() {
		setName("Kafka sampler");
	}
	
	@Override
	public SampleResult sample(Entry entry) {
		SampleResult result = new SampleResult();
		result.setSampleLabel(getName());
		try {
			result.sampleStart();
			Producer<String, String> producer = getProducer();
			KeyedMessage<String, String> msg = new KeyedMessage<String, String>(getTopic(), getMessage());
			producer.send(msg);
			result.sampleEnd(); 
			result.setSuccessful(true);
			result.setResponseCodeOK();
		} catch (Exception e) {
			result.sampleEnd(); // stop stopwatch
			result.setSuccessful(false);
			result.setResponseMessage("Exception: " + e);
			// get stack trace as a String to return as document data
			java.io.StringWriter stringWriter = new java.io.StringWriter();
			e.printStackTrace(new java.io.PrintWriter(stringWriter));
			result.setResponseData(stringWriter.toString(), null);
			result.setDataType(org.apache.jmeter.samplers.SampleResult.TEXT);
			result.setResponseCode("FAILED");
		}
		return result;
	}

	private Producer<String, String> getProducer() {
		String threadGrpName = getThreadName();
		Producer<String, String> producer = producers.get(threadGrpName);
		if(producer == null) {
			log.info(MessageFormat.format("Cannot find the producer for {0}, going to create a new producer.", threadGrpName));
			Properties props = new Properties();
			props.put("metadata.broker.list", getBrokers());
			props.put("serializer.class", getMessageSerializer());
			props.put("key.serializer.class", getKeySerializer());
			props.put("request.required.acks", "0");
			producer = new Producer<String,String>(new ProducerConfig(props));
			producers.put(threadGrpName, producer);
		}
		return producer;
	}
	
	public String getBrokers() {
		return getPropertyAsString(KAFKA_BROKERS);
	}

	public void setBrokers(String brokers) {
		setProperty(KAFKA_BROKERS, brokers);
	}

	public String getTopic() {
		return getPropertyAsString(KAFKA_TOPIC);
	}

	public void setTopic(String topic) {
		setProperty(KAFKA_TOPIC, topic);
	}

//	public String getKey() {
//		return getPropertyAsString(KAFKA_KEY);
//	}
//
//	public void setKey(String key) {
//		setProperty(KAFKA_KEY, key);
//	}

	public String getMessage() {
		return getPropertyAsString(KAFKA_MESSAGE);
	}

	public void setMessage(String message) {
		setProperty(KAFKA_MESSAGE, message);
	}

	public String getMessageSerializer() {
		return getPropertyAsString(KAFKA_MESSAGE_SERIALIZER);
	}

	public void setMessageSerializer(String messageSerializer) {
		setProperty(KAFKA_MESSAGE_SERIALIZER, messageSerializer);
	}

	public String getKeySerializer() {
		return getPropertyAsString(KAFKA_KEY_SERIALIZER);
	}

	public void setKeySerializer(String keySerializer) {
		setProperty(KAFKA_KEY_SERIALIZER, keySerializer);
	}

	@Override
	public void testEnded() {
		this.testEnded("local");
	}

	@Override
	public void testEnded(String arg0) {
//		Iterator<Producer<String, String>> it = producers.values().iterator();
//		while(it.hasNext()) {
//			Producer<String, String> producer = it.next();
//			producer.close();
//		}
	}

	@Override
	public void testStarted() {
	}

	@Override
	public void testStarted(String arg0) {
	}

	
}
