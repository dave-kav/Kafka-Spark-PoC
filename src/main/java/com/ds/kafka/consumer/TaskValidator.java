package com.ds.kafka.consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.ds.properties.Connection;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class TaskValidator implements Runnable {

	private String inputTopic;
	private List<String> outputTopicList;
	private ConsumerConnector consumer;
	private Properties props;
	private Producer<String, String> producer;
	
	private enum Coin {
		HEADS,
		TAILS
	}
	
	public TaskValidator(String inputTopic, List<String> outputTopicsSet) {
		props = Connection.getProperties();
		producer = new KafkaProducer <String, String>(props);
		consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
		this.inputTopic = inputTopic;
		this.outputTopicList = outputTopicsSet; 		
	}
	
	private Coin coinFlip() {
		return new Random().nextInt(2) == 0 ? Coin.HEADS : Coin.TAILS;
	}

	private void writeToTopic(String message) {
		if (coinFlip() == Coin.HEADS) {
			producer.send(new ProducerRecord<String, String>(outputTopicList.get(0), message));
			System.out.println(String.format("Writing to %s\n%s", outputTopicList.get(0), message));
		}
		else {
			producer.send(new ProducerRecord<String, String>(outputTopicList.get(1), message));
			System.out.println(String.format("Writing to %s\n%s", outputTopicList.get(1), message));
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void run() {
		Map<String, Integer> topicCount = new HashMap<String, Integer>();
        topicCount.put(inputTopic, 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
        List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(inputTopic);
        
        for (final KafkaStream stream : streams) {
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            while (it.hasNext()) {
            	try {
            		Thread.sleep(5000);
            		} catch (InterruptedException e) {
            			e.printStackTrace();
            		}
            	System.out.println(new String(it.next().message()));
                writeToTopic(new String(it.next().message()));
            }
        }
        
        if (consumer != null) 
        	consumer.shutdown();
        
        producer.close();
        
	}

	public static void main(String [] args) {
		if(args.length < 2){
			System.err.println("Usage: TaskValidator <input-topic> <output-topics>");
			System.exit(1);
		}

		String inputTopic = args[0];
		String outputTopics = args[1];

		List<String> outputTopicList = new ArrayList<String>(Arrays.asList(outputTopics.split(",")));
		
		if(outputTopicList.size() != 2) {
			System.err.println("Please enter exactly 2 output topics, separated by ','");
			System.exit(1);
		}
		
		new TaskValidator(inputTopic, outputTopicList).run();
	}
	
}
