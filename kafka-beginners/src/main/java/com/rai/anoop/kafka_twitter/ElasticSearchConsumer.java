package com.rai.anoop.kafka_twitter;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class ElasticSearchConsumer {

	public static RestHighLevelClient createClient() {
		String hostname = "kafka-course-6166376816.ap-southeast-2.bonsaisearch.net";
		String username = "username";
		String password = "password";
		
		// don't do if you run a local ES
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,password));
		
		RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				});
		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
	}
	
	public static KafkaConsumer<String, String> createConsumer(String topic) {
		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "kafka-demo-elasticsearch";
		
		//create consumer  configs
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
		
		//create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Arrays.asList(topic));
		
		return consumer;
	}
	
	public static void main (String[] args) throws IOException {
		Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
		RestHighLevelClient client = createClient();
		
		KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			Integer recordsCount = records.count();
			logger.info("Received " + recordsCount + " records");
			
			BulkRequest bulkRequest = new BulkRequest();
			
			for(ConsumerRecord<String, String> record: records) {
				// 2 strategies
				//String id = record.topic() + "_" + record.partition() + "_" + record.offset();
				try {
					// Twitter feed specific id
					String id = extractIdFromTweet(record.value());
					
					// here we insert data into ElasticSearch
					IndexRequest indexRequest = new IndexRequest(
							"twitter", 
							"tweets", 
							id // this is to make our consumer idempotent
					).source(record.value(), XContentType.JSON);
					
					bulkRequest.add(indexRequest); // we add to our bulk request (takes no time)
					
//					IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
//					//String id = indexResponse.getId();
//					logger.info(indexResponse.getId());
//					try {
//						Thread.sleep(1000); //just to see what is happening above, so introduced small delay
//					} catch (InterruptedException e) {
//						e.printStackTrace();
//					}
				} catch (NullPointerException e) {
					logger.warn("Skipping bad data " + record.value()); // sometimes we may receive tweet json that does not have id_str field
				}

			}
			if(recordsCount > 0) {
				BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT); // inserting 100 records into ElasticSearch at one go
				logger.info("Committing offsets...");
				consumer.commitSync(); // commit the offsets in a synchronous manner
				logger.info("Offsets have been committed");
				try {
					Thread.sleep(1000); //just to see what is happening above, so introduced small delay
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
		// close the client gracefully
		// client.close();
	}

	private static JsonParser jsonParser = new JsonParser();
	private static String extractIdFromTweet(String tweetJson) {
		// gson library
		return jsonParser.parse(tweetJson)
				.getAsJsonObject()
				.get("id_str")
				.getAsString();
	}
}
