package org.logstashplugins;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.shaded.guava.common.base.Stopwatch;

import co.elastic.logstash.api.Event;

public class CassandraDatabaseOperations
{
	private CqlSession session;

	private String dataCenter;
	
	private Map<Set<String>, List<BoundStatement>> statementBatches; // holds batches of statements subdivided per unique tags combinations (optimized for case of >1 node clusters)

	private final SimpleStatement regularInsertStatement;

	private final InetSocketAddress cassandraIP;

	private PreparedStatement sessionPreparedStatement;
	
	public CassandraDatabaseOperations(String aHost, int aPort, String aDataCenter)
	{
		this.dataCenter = aDataCenter;
		this.session = null;
		this.statementBatches = new HashMap<Set<String>, List<BoundStatement>>();
		
		regularInsertStatement = QueryBuilder.insertInto("\"event\"")
					                    .value("uid", QueryBuilder.bindMarker())
					                    .value("tags", QueryBuilder.bindMarker())
					                    .value("created", QueryBuilder.bindMarker())
										.value("embed", QueryBuilder.bindMarker())
					                    .value("data", QueryBuilder.bindMarker()).build();
		cassandraIP = new InetSocketAddress(aHost, aPort);
	}
	
	private void ensureConnected(Logger log)
	{
		if (session == null)
		{
			log.warn("Building new Cassandra session.");
			session = CqlSession.builder().addContactPoint(cassandraIP).withLocalDatacenter(dataCenter).withKeyspace("eventstore").build();
			sessionPreparedStatement = session.prepare(regularInsertStatement);
		}		
	}
	
	public void prepareEventInsertion(Event event, Logger log) throws Exception
	{
        String lastQuery = "";
        
		try
		{
			ensureConnected(log);
			
	        Object uIdField = event.getField("uid");
	        Object tagField = event.getField("tags");
	        Object createdField = event.getField("created");
	        Object embedField = event.getField("embed");
	        Object dataField = event.getField("data");
	        
	        if (uIdField == null || tagField == null || createdField == null || embedField == null || dataField == null)
	        {
	        	log.warn("Event dropped! One or more of required fields are null. uid: '"+uIdField+"' tag: '"+tagField+"' created: '"+createdField+"' data: '"+dataField+"' embed: '"+embedField+"'");
	        	return;
	        }
	        
			UUID uid = UUID.fromString(uIdField.toString());	        
			Set<String> tags = new HashSet<String>((List<String>)tagField);	        
			Instant created = Instant.parse(createdField.toString());
			String embed = embedField.toString();	        
			String data = dataField.toString();
	        
	        lastQuery = "INSERT INTO eventstore.evet (uid, tags, created, embed, data) VALUES ("+uid+", "+tags+", "+created+", "+embed+", "+data+")";
	        			
			BoundStatement boundStatement = sessionPreparedStatement.bind()
					.setUuid(0, uid)
					.setSet(1, tags, String.class)
					.setInstant(2, created)
					.setString(3, embed)
					.setString(4, data);
			
			if (!statementBatches.containsKey(tags))
			{
				statementBatches.put(tags, new ArrayList<BoundStatement>());
			}
			
			statementBatches.get(tags).add(boundStatement);
		}
		catch (Exception e)
		{
			log.error("Exception "+e+" caused when preparing query: "+lastQuery, e);
			
			if (session!=null) session.close();
			session = null;
			sessionPreparedStatement = null;
			statementBatches.clear();
			throw e;
		}
	}

	public void storeAllEvents(Logger log)
	{
        Stopwatch sw = Stopwatch.createUnstarted();
        int groupCounter = 0;
        int eventCount = 0;
        
		try
		{		
			sw.start();
			
			for(List<BoundStatement> bss : statementBatches.values())
			{
				++groupCounter;
				eventCount += bss.size();
				
				BatchStatement bs = BatchStatement.newInstance(BatchType.UNLOGGED).addAll(bss);
				
				ResultSet rs = session.execute(bs);
				
				ExecutionInfo ei = rs.getExecutionInfo();
				for (String warning : ei.getWarnings())
				{
					log.warn(warning);
				}
			}
			
			sw.stop();
		}
		catch (Exception e)
		{
			if (sw.isRunning()) { sw.stop(); };
			log.error("Exception caused by query executed in "+sw.elapsed(TimeUnit.MILLISECONDS)+"ms.", e);
			
			if (session!=null) session.close();
			session = null;
			throw e;
		}
		finally
		{
			statementBatches.clear();
		}
		
		if (eventCount > 0)
		{
			log.info("Stored "+eventCount+" events within "+groupCounter+" group(s) in "+sw.elapsed(TimeUnit.MILLISECONDS)+"ms.");
		}
	}

}
