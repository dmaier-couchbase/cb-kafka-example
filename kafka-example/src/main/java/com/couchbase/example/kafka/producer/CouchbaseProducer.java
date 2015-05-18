package com.couchbase.example.kafka.producer;

import com.couchbase.kafka.CouchbaseKafkaConnector;
import com.couchbase.kafka.DefaultCouchbaseKafkaEnvironment;

/**
 *  The Couchbase Producer publishes Mutation Messages from Couchbase to Kafka
 * 
 * @author David Maier <david.maier at couchbase.com>
 */
public class CouchbaseProducer implements IProducer {

    /**
     * The bucket to connect to
     */
    private final String bucket;
    
    /**
     * The password
     */
    private final String password;
   
    /**
     * One Couchbase cluster host
     */
    private final String node;
    
    /**
     * The zookeeper
     */
    private final String zookeeper;
    
    /**
     * The Kafka topic
     */
    private final String topic;
    
    /**
     * The inner Kafka connector
     */
    private final CouchbaseKafkaConnector innerConnector;
    
    
    
    /**
     * The constructor which takes all connection details as arguments
     * 
     * @param couchbaseNode
     * @param bucket
     * @param password
     * @param zookeeper
     * @param topic 
     */
    public CouchbaseProducer(String couchbaseNode, String bucket, String password, String zookeeper, String topic) {
    
        this.node = couchbaseNode;
        this.bucket = bucket;
        this.password = password;
        this.zookeeper = zookeeper;
        this.topic = topic;
        
        
        
        DefaultCouchbaseKafkaEnvironment.Builder envBuilder = (DefaultCouchbaseKafkaEnvironment.Builder) DefaultCouchbaseKafkaEnvironment
                        .builder()
                        .kafkaFilterClass(MutationMessageFilter.class.getName())
                        .kafkaValueSerializerClass(MutationMessageEncoder.class.getName())
                        .dcpEnabled(true);
        
        //-- The envrionment is optional
        //Per default com.couchbase.kafka.coder.JsonEncoder is used for the value serialization
        innerConnector = CouchbaseKafkaConnector.create(envBuilder.build(), this.node, this.bucket, this.password, this.zookeeper , this.topic);
    }

    @Override
    public void produce() {
    
        innerConnector.run();
    }
    
    /**
     * We want to start this Producer from the command line
     * 
     * @param args 
     */
    public static void main(String[] args) {
        
        if (args != null && args.length == 5)
        {
            String pNode = args[0];
            String pBucket = args[1];
            String pPassword = args[2];
            String pZookeeper = args[3];
            String pTopic = args[4];
            
            CouchbaseProducer producer = new CouchbaseProducer(pNode, pBucket, pPassword, pZookeeper, pTopic);
            producer.produce();
        }
        else
        {
            System.out.println("Usage: java com.couchbase.example.kafka.producer.CouchbaseProducer ");
            System.out.println("$couchbaseNode $bucket $password $zookeeper $topic");
        }
    }
    
}
