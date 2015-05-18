package com.couchbase.example.kafka.producer;

import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.kafka.DCPEvent;

//Provided by the Couchbase Kafka connector
import com.couchbase.kafka.filter.Filter;

/**
 * A filter on Mutation Messages
 * 
 * @author David Maier <david.maier at couchbase.com>
 */
public class MutationMessageFilter implements Filter
{

    @Override
    public boolean pass(DCPEvent dcpe) {
      
        //Only handle mutation messages
        return dcpe.message() instanceof MutationMessage;
    }
    
}
