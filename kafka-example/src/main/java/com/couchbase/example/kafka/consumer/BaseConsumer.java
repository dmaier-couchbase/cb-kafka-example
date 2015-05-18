package com.couchbase.example.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;

//Use Java instead Scala
import kafka.javaapi.consumer.ConsumerConnector;



/**
 * A simple consumer
 * 
 * @author David Maier <david.maier at couchbase.com>
 */
public abstract class BaseConsumer {

    //Constants
    public static final int TIMEOUT = 500;
    public static final int SYNC_TIME = 250;
    public static final int COMMIT_INTV = 1000;
    
    /**
     * The inner consumer connector
     */
    protected final ConsumerConnector innerConsumer;
    
    /**
     * The topic to consume
     */
    protected final String topic;
    
    /**
     * The map which stores how many threads should be used per topic
     */
    protected final Map<String, Integer> topicMap = new HashMap<>();
    
    /**
     * The map which contains the streams per topic
     */
    protected final Map<String, List<KafkaStream< byte[], byte[]>>> streams; 
    
    /**
     * The constructor which takes the ZooKeeper, the consumer group 
     * and the topic details as arguments
     * 
     * @param zookeeper
     * @param groupId
     * @param topic 
     */
    public BaseConsumer(String zookeeper, String groupId, String topic) {
    
        Properties props = new Properties();
        
        props.put("zookeeper.connect", zookeeper);
        props.put("zookeeper.session.timeout.ms", ""+TIMEOUT);
        props.put("zookeeper.sync.time.ms", ""+SYNC_TIME);
        
        props.put("auto.commit.interval.ms", ""+COMMIT_INTV);
        
        //This is the consumer group
        props.put("group.id", groupId);
        
        
        ConsumerConfig cfg = new ConsumerConfig(props);
    
        //Use Java instead Scala
        this.innerConsumer = Consumer.createJavaConsumerConnector(cfg);
        this.topic = topic;
        this.topicMap.put(topic, 1);
        this.streams = innerConsumer.createMessageStreams(topicMap);
    }
    
    /**
     * The consumption function
     */
    abstract public void consume();
    
    /**
     * Shutdown the consumer
     */
    protected void shutdown()
    {
         if (innerConsumer != null) innerConsumer.shutdown();
    }
     
}
