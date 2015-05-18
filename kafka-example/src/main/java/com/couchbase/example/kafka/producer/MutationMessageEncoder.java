package com.couchbase.example.kafka.producer;

//Provided by the Kafka connector
import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.couchbase.kafka.DCPEvent;
import com.couchbase.kafka.coder.AbstractEncoder;
import kafka.utils.VerifiableProperties;

/**
 *
 * @author David Maier <david.maier at couchbase.com>
 */
public class MutationMessageEncoder extends AbstractEncoder {

    /**
     * Inherited constructor
     * 
     * @param properties 
     */
    public MutationMessageEncoder(VerifiableProperties properties) {
        super(properties);
    }

    /**
     * To transfer the mutation message into bytes
     * 
     * @param dcpe
     * @return 
     */
    @Override
    public byte[] toBytes(DCPEvent dcpe) {
        
        MutationMessage cbmsg = (MutationMessage) dcpe.message();
        
        String data = "key = " +  cbmsg.key() + ", cas = " +  cbmsg.cas();
        
        return data.getBytes(CharsetUtil.UTF_8);
    }    
}
