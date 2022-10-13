package org.logstashplugins;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.Logger;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.Output;
import co.elastic.logstash.api.PluginConfigSpec;

@LogstashPlugin(name = "cassandra_load")
public class CassandraLoad implements Output
{
	public static final PluginConfigSpec<String> CASSANDRA_HOST_CONFIG = PluginConfigSpec.stringSetting("cassandrahost", "192.168.1.14");
	
	public static final PluginConfigSpec<String> CASSANDRA_DATACENTER_CONFIG = PluginConfigSpec.stringSetting("cassandradatacenter", "datacenter1");
	
	public static final PluginConfigSpec<Long> CASSANDRA_PORT_CONFIG = PluginConfigSpec.numSetting("cassandraport", 9042);

    private final String id;    
    private final CountDownLatch done = new CountDownLatch(1);
    private volatile boolean stopped = false;
    private Logger log;
    
	private String cassandraHost;
	private int cassandraPort;
	private String cassandraDatacenter;
    
    private CassandraDatabaseOperations cdo = null;

    public CassandraLoad(final String id, final Configuration config, final Context context)
    {    
    	this.id = id;
    	this.log = context.getLogger(this);
    	this.cassandraHost = config.get(CASSANDRA_HOST_CONFIG);
    	this.cassandraPort = config.get(CASSANDRA_PORT_CONFIG).intValue();
    	this.cassandraDatacenter = config.get(CASSANDRA_DATACENTER_CONFIG);
    	
    	log.info("Plugin CassandraLoad starting.");
    	
        try
        {
        	this.cdo = new CassandraDatabaseOperations(cassandraHost, cassandraPort, cassandraDatacenter);
        }
    	catch (Exception ex)
        {
    		this.cdo = null;
    		log.error("Error when initializing CassandraLoad: ", ex);
        }    	
    }

    @Override
    public void output(final Collection<Event> events)
    {
    	try
    	{
	    	if (this.cdo == null)
	    	{
	    		this.cdo = new CassandraDatabaseOperations(cassandraHost, cassandraPort, cassandraDatacenter);
	    	}
	    	
	        Iterator<Event> z = events.iterator();
	        while (z.hasNext() && !stopped)
	        {
	        	Event event = z.next();
				cdo.prepareEventInsertion(event, log); // prepare all events and store in one go afterwards
	        }
	    	
	        cdo.storeAllEvents(log); // store all at once now
    	}
    	catch (Exception ex)
        {
    		this.cdo = null;
    		log.error("Error when storing events: ", ex);
        }
    }

    @Override
    public void stop()
    {
    	log.info("Plugin CassandraLoad stopping.");
        stopped = true;
        done.countDown();
    }

    @Override
    public void awaitStop() throws InterruptedException
    {
        done.await();
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema()
    {
        return Arrays.asList(CASSANDRA_HOST_CONFIG, CASSANDRA_DATACENTER_CONFIG, CASSANDRA_PORT_CONFIG);
    }

    @Override
    public String getId()
    {
        return id;
    }
}
