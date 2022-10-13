package org.logstashplugins;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Test;
//import co.elastic.logstash.api.Event;
import org.logstash.Event;
import org.logstash.plugins.ConfigurationImpl;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.DeadLetterQueueWriter;
import co.elastic.logstash.api.EventFactory;
import co.elastic.logstash.api.NamespacedMetric;
import co.elastic.logstash.api.Plugin;

public class JavaOutputExampleTest
{
	
	{
		Configurator.setRootLevel(Level.DEBUG);
	}
	private static Logger log = LogManager.getLogger("JavaOutputExampleTest");	

    @Test
    public void testJavaOutputExample()
    {
        Map<String, Object> configValues = new HashMap<>();
        Configuration config = new ConfigurationImpl(configValues);
        Context context = new Context()
        {
			@Override
			public NamespacedMetric getMetric(Plugin arg0) { return null; }
			@Override
			public Logger getLogger(Plugin arg0) { return log; }
			@Override
			public EventFactory getEventFactory() { return null; }
			@Override
			public DeadLetterQueueWriter getDlqWriter() { return null; }
		};
        
        CassandraLoad output = new CassandraLoad("test-id", config, context);

        //select * from event where TAGS = {'plugin', 'test'};
        
        String jsonData =  "[{\"created\":\"2020-03-03T00:21:08.427Z\",\"uid\":\"4b6ff38c-a241-4c0c-9874-390a3a8f02e3\",\"embed\":\"{\\\"document_id\\\":\\\"H4XEcGoBygQ0YUNpN7o0\\\"}\",\"tags\":[\"test\",\"plugin\"],\"data\":\"{\\\"counter\\\":17496,\\\"voltage\\\":8.22,\\\"temperature\\\":25,\\\"humidity\\\":45,\\\"device\\\":\\\"Sensor 1E950F\\\",\\\"light\\\":0.19}\"}," + 
			        		"{\"created\":\"2020-03-03T00:22:43.957Z\",\"uid\":\"ce2a383f-d430-44f2-8bd0-c00f442974f4\",\"embed\":\"{\\\"document_id\\\":\\\"KIXFcGoBygQ0YUNprLpd\\\"}\",\"tags\":[\"test\",\"plugin\"],\"data\":\"{\\\"counter\\\":17505,\\\"voltage\\\":8.22,\\\"temperature\\\":25,\\\"humidity\\\":45,\\\"device\\\":\\\"Sensor 1E950F\\\",\\\"light\\\":0.19}\"}," + 
			        		"{\"created\":\"2020-03-03T00:23:26.412Z\",\"uid\":\"f7cdab54-c21e-48c1-9283-6771757100c8\",\"embed\":\"{\\\"document_id\\\":\\\"LIXGcGoBygQ0YUNpUro1\\\"}\",\"tags\":[\"test\",\"plugin\"],\"data\":\"{\\\"counter\\\":17509,\\\"voltage\\\":8.22,\\\"temperature\\\":25,\\\"humidity\\\":45,\\\"device\\\":\\\"Sensor 1E950F\\\",\\\"light\\\":0.19}\"}," + 
			        		"{\"created\":\"2020-03-03T00:31:55.883Z\",\"uid\":\"1cbf478d-8f7b-4666-b0cc-d112bf506981\",\"embed\":\"{\\\"document_id\\\":\\\"XIXOcGoBygQ0YUNpGLpT\\\"}\",\"tags\":[\"test\",\"plugin\"],\"data\":\"{\\\"counter\\\":17557,\\\"voltage\\\":8.22,\\\"temperature\\\":25,\\\"humidity\\\":45,\\\"device\\\":\\\"Sensor 1E950F\\\",\\\"light\\\":0.19}\"}," + 
			        		"{\"created\":\"2020-03-03T00:33:20.790Z\",\"uid\":\"1cd60018-0a95-4024-b5d4-72a1c4f775f3\",\"embed\":\"{\\\"document_id\\\":\\\"ZIXPcGoBygQ0YUNpY7r-\\\"}\",\"tags\":[\"test\",\"plugin\"],\"data\":\"{\\\"counter\\\":17565,\\\"voltage\\\":8.22,\\\"temperature\\\":25,\\\"humidity\\\":45,\\\"device\\\":\\\"Sensor 1E950F\\\",\\\"light\\\":0.19}\"}," + 
			        		"{\"created\":\"2020-03-03T00:35:28.149Z\",\"uid\":\"0dc2c313-c8e4-46b3-8197-5f9161935c04\",\"embed\":\"{\\\"document_id\\\":\\\"cIXRcGoBygQ0YUNpVbp9\\\"}\",\"tags\":[\"test\",\"plugin\"],\"data\":\"{\\\"counter\\\":17577,\\\"voltage\\\":8.22,\\\"temperature\\\":25,\\\"humidity\\\":45,\\\"device\\\":\\\"Sensor 1E950F\\\",\\\"light\\\":0.19}\"}," + 
			        		"{\"created\":\"2020-03-03T00:36:53.059Z\",\"uid\":\"424c4ac5-68e5-4b56-8bbc-0360bf7cbe0c\",\"embed\":\"{\\\"document_id\\\":\\\"eIXScGoBygQ0YUNpobor\\\"}\",\"tags\":[\"test\",\"plugin\"],\"data\":\"{\\\"counter\\\":17585,\\\"voltage\\\":8.22,\\\"temperature\\\":25,\\\"humidity\\\":45,\\\"device\\\":\\\"Sensor 1E950F\\\",\\\"light\\\":0.19}\"}," + 
			        		"{\"created\":\"2020-03-03T00:37:03.670Z\",\"uid\":\"0de8e930-0da5-44fb-8eea-d50e8587cabf\",\"embed\":\"{\\\"document_id\\\":\\\"eYXScGoBygQ0YUNpyrqf\\\"}\",\"tags\":[\"test\",\"plugin\"],\"data\":\"{\\\"counter\\\":17586,\\\"voltage\\\":8.33,\\\"temperature\\\":25,\\\"humidity\\\":45,\\\"device\\\":\\\"Sensor 1E950F\\\",\\\"light\\\":0.19}\"}," + 
			        		"{\"created\":\"2020-03-03T00:37:35.513Z\",\"uid\":\"7dcd621d-afe7-4353-a020-46b5fd4e3306\",\"embed\":\"{\\\"document_id\\\":\\\"fIXTcGoBygQ0YUNpR7oB\\\"}\",\"tags\":[\"test\",\"plugin\"],\"data\":\"{\\\"counter\\\":17589,\\\"voltage\\\":8.22,\\\"temperature\\\":25,\\\"humidity\\\":45,\\\"device\\\":\\\"Sensor 1E950F\\\",\\\"light\\\":0.19}\"}," + 
			        		"{\"created\":\"2020-03-03T00:39:00.422Z\",\"uid\":\"ef10f66c-b41b-42ba-8f37-44e8ea68c437\",\"embed\":\"{\\\"document_id\\\":\\\"hIXUcGoBygQ0YUNpkrqu\\\"}\",\"tags\":[\"test\",\"plugin\"],\"data\":\"{\\\"counter\\\":17597,\\\"voltage\\\":8.33,\\\"temperature\\\":25,\\\"humidity\\\":45,\\\"device\\\":\\\"Sensor 1E950F\\\",\\\"light\\\":0.19}\"}," + 
			        		"{\"created\":\"2020-03-03T00:39:11.036Z\",\"uid\":\"ce04c9b2-b0b5-4f3d-a330-1186256ecc55\",\"embed\":\"{\\\"document_id\\\":\\\"hYXUcGoBygQ0YUNpvLok\\\"}\",\"tags\":[\"test\",\"plugin\"],\"data\":\"{\\\"counter\\\":17598,\\\"voltage\\\":8.22,\\\"temperature\\\":25,\\\"humidity\\\":45,\\\"device\\\":\\\"Sensor 1E950F\\\",\\\"light\\\":0.19}\"}]";
        Collection<co.elastic.logstash.api.Event> events;
		try
		{
			events = new ArrayList<>();			
			List<Event> eventList = Arrays.asList(Event.fromJson(jsonData));
			
			for (int i = 0; i < 10000; i++)
			{
				eventList.get(0).setField("uid", UUID.randomUUID().toString());
				events.add(eventList.get(0).clone());
			}
			
			//events = new ArrayList<>(eventList);					
			
			output.output(events);
			output.stop();
			output.awaitStop();
		} 
		catch (IOException e1)
		{
			e1.printStackTrace();
		}
        catch (InterruptedException e)
        {		
			e.printStackTrace();
		}
    }
}
