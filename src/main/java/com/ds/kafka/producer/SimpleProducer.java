package com.ds.kafka.producer;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.ds.properties.Connection;
import com.ds.result.Result;
import com.ds.result.ResultFactory;
import com.ds.result.Teams;
import com.ds.serializer.ResultSerializer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class SimpleProducer {

	public static void main(String[] args) throws Exception {

		// Check arguments length value
		if(args.length < 1){
			System.err.println("Enter topic name");
			System.exit(1);;
		}

		String topicName = args[0].toString();
		Properties props = Connection.getProperties();
		Producer<String, String> producer = new KafkaProducer <String, String>(props);

		List<String> teams = null;
		try {
			teams = Teams.getTeams();
		} catch (IOException e) {
			e.printStackTrace();
		}

		boolean running = true;
		int i = 0;
		while (running) {
			
			//generate result
			Result result = ResultFactory.generateResult(teams);
			
			//serialize to JSON
			Gson gson = new GsonBuilder()
					.registerTypeAdapter(Result.class, new ResultSerializer())
					.create();
			
			//write to kafka topic
			producer.send(new ProducerRecord<String, String>(topicName, gson.toJson(result)));
			System.out.println(String.format("Result %d Posted from source: %s", 
												++i,gson.toJson(result.toString())));

			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		producer.close();
	}
}