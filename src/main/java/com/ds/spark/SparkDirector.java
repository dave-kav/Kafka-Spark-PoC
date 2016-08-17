package com.ds.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;

import com.ds.properties.Connection;
import com.ds.result.Result;
import com.ds.serializer.ResultDeserializer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import kafka.serializer.StringDecoder;

public class SparkDirector {

	private static KieServices ks;
	private static KieContainer kContainer;

	public static void main(String[] args) throws Exception {

		if (args.length < 4) {
			StringBuilder sb = new StringBuilder();
			sb.append("Usage: ResultReader <brokers> <input-topics> <output-topics> <patterns>\n");
			sb.append("\t<brokers> is a list of one or more Kafka brokers\n");
			sb.append("\t<input-topics> is a list of one or more kafka topics to consume from\n");
			sb.append("\t<output-topics> is a list of one or more kafka topics to write to\n");
			sb.append("\t<patterns> is a list of one or more patterns that determine the data output to a topic\n");
			sb.append("\t**patterns must be entered in the same order as the topics they relate to**\n\n");
			System.err.println(sb);
			System.exit(1);
		}

		//parse command line parameters
		String brokers = args[0];
		String inputTopics = args[1];
		Set<String> inputTopicsSet = new HashSet<String>(Arrays.asList(inputTopics.split(",")));
		String outputTopics = args[2];
		List<String> outputTopicsSet = new ArrayList<String>(Arrays.asList(outputTopics.split(",")));
		String patterns = args[3];
		List<String> patternSet = new ArrayList<String>(Arrays.asList(patterns.split(",")));

		if (outputTopicsSet.size() != patternSet.size()) {
			System.err.println("Please ensure there is an equal amount of <output-topics> and <patterns>\n\n" + 
					"  **patterns must be entered in the same order as the topics they relate to**\n\n");
			System.exit(1);
		}

		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);
		String checkpointDir = "/home/dkh2/DevTools/checkpointDir";
		JavaStreamingContextFactory contextFactory = getStreamingContextFactory(checkpointDir);
		
		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = 
				KafkaUtils.createDirectStream (
						JavaStreamingContext.getOrCreate(checkpointDir, contextFactory),
						String.class,
						String.class,
						StringDecoder.class,
						StringDecoder.class,
						kafkaParams,
						inputTopicsSet
						);

		//process stream and parse JSON, fire rules
		JavaDStream<String> linesAsJson = messages.map(tuple2 -> tuple2._2());
		JavaDStream<String> resultStream = linesAsJson.map(new Function<String,String>() {
			private static final long serialVersionUID = -8706635003107925008L;

			@Override
			public String call(String json) throws Exception {
				Gson gson = getGson();
				KieSession kSession = getKies();
				Result result = gson.fromJson(json, Result.class);
				kSession.insert(result);
				kSession.fireAllRules(1);
				return result.toString();
			}
		});

		JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(checkpointDir, contextFactory);
		
		//split data into topics as per command line params
		for (int i = 0; i < patternSet.size(); i++) {
			splitStream(resultStream, patternSet.get(i), outputTopicsSet.get(i));
		}

		jssc.start();
		jssc.awaitTermination();
	}		

	/**
	 * Filter a dstream and write to relevant topic
	 * @param resultStream
	 * @param result
	 * @param topic
	 */
	private static void splitStream(JavaDStream<String> resultStream, String result, String topic) {
		JavaDStream<String> filteredStream = resultStream.filter(s -> s.contains(result));
		Properties props = Connection.getProperties();
		props.put("topic", topic);

		filteredStream.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
			private static final long serialVersionUID = 4882660939889945052L;
			//remove any duplicates
			@Override
			public JavaRDD<String> call(JavaRDD<String> rows) throws Exception {
				return rows.distinct();
			}
		}).foreachRDD(new Function<JavaRDD<String>,Void>() {
			private static final long serialVersionUID = -466819591844174799L;
			//write filtered data to relevant topic
			@Override
			public Void call(JavaRDD<String> rdd) throws Exception {

				Producer<String, String> producer = new KafkaProducer <String, String>(props);
				List<String> rdds = rdd.collect();
				for (String s: rdds) {
					producer.send(new ProducerRecord<String, String>(topic, s.toString()));
				}
				producer.close();
				return null;
			}
		});
	}

	private static JavaStreamingContextFactory getStreamingContextFactory(String checkpointDir) {
		JavaStreamingContextFactory contextFactory = new JavaStreamingContextFactory() {
			@Override 
			public JavaStreamingContext create() {
				//set up spark streaming context
				SparkConf sparkConf = new SparkConf()
						.setAppName("JavaScores")
						.setMaster("local[2]");
				JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));
				jssc.checkpoint(checkpointDir);                       // set checkpoint directory
				return jssc;
			}
		};
		return contextFactory;
	}

	/**
	 * Gson setup
	 * @return gsonBuilder
	 */
	private static Gson getGson() {
		GsonBuilder gsonBuilder = new GsonBuilder();
		gsonBuilder.registerTypeAdapter(Result.class, new ResultDeserializer());
		return gsonBuilder.create();
	}

	/**
	 * Drools environment set-up
	 * @return KieSession 
	 */
	private static KieSession getKies() {
		ks = KieServices.Factory.get();
		kContainer = ks.getKieClasspathContainer();
		return kContainer.newKieSession("ksession-rules");
	}
}
