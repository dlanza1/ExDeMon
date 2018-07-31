package ch.cern.exdemon.json;

import org.junit.Assert;
import org.junit.Test;

public class JSONObjectParserTest {
	
	@Test
	public void establishEvent(){
		
		String json = "{\"event_timestamp\":\"2016-11-02T09:12:09+0100\","
				+ "\"text\":\"02-NOV-2016 09:12:09 * (CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=timtest_s.cern.ch)"
						+ "(FAILOVER_MODE=(TYPE=SELECT)(METHOD=BASIC)(RETRIES=200)(DELAY=15))"
						+ "(CID=(PROGRAM=perl)(HOST=itrac50044.cern.ch)(USER=oracle))(INSTANCE_NAME=ACCINT2)) "
					+ "* (ADDRESS=(PROTOCOL=tcp)(HOST=10.16.8.163)(PORT=57543)) "
					+ "* establish * timtest_s.cern.ch "
					+ "* 0\","
				+ "\"CONNECT_DATA_SERVER\":\"DEDICATED\","
				+ "\"CONNECT_DATA_SERVICE_NAME\":\"timtest_s.cern.ch\","
				+ "\"CONNECT_DATA_FAILOVER_MODE_TYPE\":\"SELECT\","
				+ "\"CONNECT_DATA_FAILOVER_MODE_METHOD\":\"BASIC\","
				+ "\"CONNECT_DATA_FAILOVER_MODE_RETRIES\":200,"
				+ "\"CONNECT_DATA_FAILOVER_MODE_DELAY\":15,"
				+ "\"client_program\":\"perl\","
				+ "\"client_host\":\"itrac50044.cern.ch\","
				+ "\"client_user\":\"oracle\","
				+ "\"CONNECT_DATA_INSTANCE_NAME\":\"ACCINT2\","
				+ "\"client_protocol\":\"tcp\","
				+ "\"client_ip\":\"10.16.8.163\","
				+ "\"client_port\":57543,"
				+ "\"type\":\"establish\","
				+ "\"service_name\":\"timtest_s.cern.ch\","
				+ "\"return_code\":0}";
		
		try {
			JSON listenerEvent = new JSON.Parser().parse(json.getBytes());
			
			Assert.assertNotNull(listenerEvent.getProperty("type"));
			Assert.assertEquals("establish", listenerEvent.getProperty("type"));
			
			Assert.assertNotNull(listenerEvent.getProperty("event_timestamp"));
			Assert.assertEquals("2016-11-02T09:12:09+0100", listenerEvent.getProperty("event_timestamp"));
			Assert.assertNotNull(listenerEvent.getProperty("text"));
			
			Assert.assertNotNull(listenerEvent.getProperty("CONNECT_DATA_SERVER"));
			Assert.assertEquals("DEDICATED", listenerEvent.getProperty("CONNECT_DATA_SERVER"));
			Assert.assertNotNull(listenerEvent.getProperty("CONNECT_DATA_SERVICE_NAME"));
			Assert.assertEquals("timtest_s.cern.ch", listenerEvent.getProperty("CONNECT_DATA_SERVICE_NAME"));
			Assert.assertNotNull(listenerEvent.getProperty("CONNECT_DATA_FAILOVER_MODE_TYPE"));
			Assert.assertEquals("SELECT", listenerEvent.getProperty("CONNECT_DATA_FAILOVER_MODE_TYPE"));
			Assert.assertNotNull(listenerEvent.getProperty("CONNECT_DATA_FAILOVER_MODE_METHOD"));
			Assert.assertEquals("BASIC", listenerEvent.getProperty("CONNECT_DATA_FAILOVER_MODE_METHOD"));
			Assert.assertNotNull(listenerEvent.getProperty("CONNECT_DATA_FAILOVER_MODE_RETRIES"));
			Assert.assertEquals("200", listenerEvent.getProperty("CONNECT_DATA_FAILOVER_MODE_RETRIES"));
			Assert.assertNotNull(listenerEvent.getProperty("CONNECT_DATA_FAILOVER_MODE_DELAY"));
			Assert.assertEquals("15", listenerEvent.getProperty("CONNECT_DATA_FAILOVER_MODE_DELAY"));
			Assert.assertNotNull(listenerEvent.getProperty("client_program"));
			Assert.assertEquals("perl", listenerEvent.getProperty("client_program"));
			Assert.assertNotNull(listenerEvent.getProperty("client_host"));
			Assert.assertEquals("itrac50044.cern.ch", listenerEvent.getProperty("client_host"));
			Assert.assertNotNull(listenerEvent.getProperty("client_user"));
			Assert.assertEquals("oracle", listenerEvent.getProperty("client_user"));
			Assert.assertNotNull(listenerEvent.getProperty("CONNECT_DATA_INSTANCE_NAME"));
			Assert.assertEquals("ACCINT2", listenerEvent.getProperty("CONNECT_DATA_INSTANCE_NAME"));
			
			Assert.assertNotNull(listenerEvent.getProperty("client_protocol"));
			Assert.assertEquals("tcp", listenerEvent.getProperty("client_protocol"));
			Assert.assertNotNull(listenerEvent.getProperty("client_ip"));
			Assert.assertEquals("10.16.8.163", listenerEvent.getProperty("client_ip"));
			Assert.assertNotNull(listenerEvent.getProperty("client_port"));
			Assert.assertEquals("57543", listenerEvent.getProperty("client_port"));
			
			Assert.assertNotNull(listenerEvent.getProperty("service_name"));
			Assert.assertEquals("timtest_s.cern.ch", listenerEvent.getProperty("service_name"));
			Assert.assertNotNull(listenerEvent.getProperty("return_code"));
			Assert.assertEquals("0", listenerEvent.getProperty("return_code"));
		} catch (Exception e) {
			e.printStackTrace();
			
			Assert.fail();
		}
		
	}
	
	@Test
	public void otherEvent(){
		
		String json = "{\"event_timestamp\":\"2016-11-02T09:12:06+0100\","
				+ "\"text\":\"02-NOV-2016 09:12:06 * unknown * other_type * 15\"}";
		
		try {
			JSON listenerEvent = new JSON.Parser().parse(json.getBytes());
			
			Assert.assertNull(listenerEvent.getProperty("type"));
		} catch (Exception e) {
			e.printStackTrace();
			
			Assert.fail();
		}
		
	}
	
}
