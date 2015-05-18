package com.couchbase.example.kafka.consumer;

import java.util.List;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

/**
 * A simple LogConsumer
 *
 * @author David Maier <david.maier at couchbase.com>
 */
public class LogConsumer extends BaseConsumer {
    
    public LogConsumer(String zookeeper, String groupId, String topic) {
        super(zookeeper, groupId, topic );
    }
    
    /**
     * Consume multi threaded by using the derived executor
     */
    @Override
    public void consume()
    {
        List<KafkaStream<byte[], byte[]>> streamList = this.streams.get(this.topic);

        for (final KafkaStream<byte[], byte[]> s : streamList) {

            ConsumerIterator<byte[], byte[]> it = s.iterator();

            while (it.hasNext()) {
                System.out.println("msg = " + new String(it.next().message()));
            }
        }
        
        shutdown();
    }
   
    
    /**
     * We want to start this consumer from the command line
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        
        if (args != null && args.length == 3)
        {
            String pZookeeper  = args[0];            
            String pGroupId = args[1];
            String pTopic = args[2];
            
            LogConsumer logConsumer = new LogConsumer(pZookeeper, pGroupId, pTopic);
            logConsumer.consume();
        }
        else
        {
            System.out.println("Usage: java com.couchbase.example.kafka.consumer.LogConsumer ");
            System.out.println("$zookeeper $groupId $topic");
        }
    }
}
