package com.ds.kafka.consumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.ds.properties.Connection;

public class SimpleConsumer {

    private final ConsumerConnector consumer;
    private final String topic;
    private int readCount = 0;

    public SimpleConsumer(String topic) {
        Properties props = Connection.getProperties();

        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        this.topic = topic;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
	public void testConsumer() {
        Map<String, Integer> topicCount = new HashMap<String, Integer>();
        topicCount.put(topic, 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
        List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);
        for (final KafkaStream stream : streams) {
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            while (it.hasNext()) {
                System.out.println(String.format("Message %d from %s: %s", ++readCount, topic, new String(it.next().message())));
            }
        }
        if (consumer != null) {
            consumer.shutdown();
        }
    }

    public static void main(String[] args) {
    	
    	if(args.length < 1){
			System.err.println("Enter topic name");
			System.exit(1);
		}
    	
        String topic = args[0];
        SimpleConsumer simpleConsumer = new SimpleConsumer(topic);
        simpleConsumer.testConsumer();
    }
}