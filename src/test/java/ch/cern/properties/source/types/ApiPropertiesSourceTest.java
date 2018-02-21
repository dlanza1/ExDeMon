package ch.cern.properties.source.types;

import ch.cern.properties.Properties;
import com.google.gson.JsonParser;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class ApiPropertiesSourceTest {
    
    private String metricResp = "{\n" +
                                "  \"metrics\": [\n" +
                                "    {\n" +
                                "      \"data\": {\n" +
                                "        \"metrics\": {\n" +
                                "          \"attribute\": {\n" +
                                "            \"$defined_metric\": \"drive_consec_failed_reqs\"\n" +
                                "          },\n" +
                                "          \"filter\": {\n" +
                                "            \"attribute\": {\n" +
                                "              \"$environment\": \"qa\",\n" +
                                "              \"$owner\": \"tape\",\n" +
                                "              \"$schema\": \"tapeserverd\",\n" +
                                "              \"$value_attribute\": \"status\",\n" +
                                "              \"clientHost\": \"!'.*repack.*'\",\n" +
                                "              \"msg\": \"Tape session finished\",\n" +
                                "              \"status\": \"'success' 'failure'\"\n" +
                                "            }\n" +
                                "          },\n" +
                                "          \"groupby\": \"$owner $environment hostname driveUnit\"\n" +
                                "        },\n" +
                                "        \"value\": \"status == 'success'\",\n" +
                                "        \"variables\": {\n" +
                                "          \"status\": {\n" +
                                "            \"filter\": {\n" +
                                "              \"attribute\": {\n" +
                                "                \"$value_attribute\": \"status\"\n" +
                                "              }\n" +
                                "            }\n" +
                                "          }\n" +
                                "        }\n" +
                                "      },\n" +
                                "      \"enabled\": true,\n" +
                                "      \"environment\": \"qa\",\n" +
                                "      \"id\": 27,\n" +
                                "      \"name\": \"drive_consec_failed_reqs\",\n" +
                                "      \"project\": \"tape\"\n" +
                                "    },\n" +
                                "    {\n" +
                                "      \"data\": {\n" +
                                "        \"metrics\": {\n" +
                                "          \"attribute\": {\n" +
                                "            \"$defined_metric\": \"perf-missing\"\n" +
                                "          },\n" +
                                "          \"filter\": {\n" +
                                "            \"attribute\": {\n" +
                                "              \"$environment\": \"qa\",\n" +
                                "              \"$owner\": \"tape\",\n" +
                                "              \"$schema\": \"perf\"\n" +
                                "            }\n" +
                                "          },\n" +
                                "          \"groupby\": \"$owner $environment hostname\"\n" +
                                "        },\n" +
                                "        \"value\": \"count\",\n" +
                                "        \"variables\": {\n" +
                                "          \"count\": {\n" +
                                "            \"aggregate\": {\n" +
                                "              \"type\": \"count_floats\"\n" +
                                "            },\n" +
                                "            \"expire\": \"2m\"\n" +
                                "          }\n" +
                                "        },\n" +
                                "        \"when\": \"BATCH\"\n" +
                                "      },\n" +
                                "      \"enabled\": true,\n" +
                                "      \"environment\": \"qa\",\n" +
                                "      \"id\": 14,\n" +
                                "      \"name\": \"perf-missing\",\n" +
                                "      \"project\": \"tape\"\n" +
                                "    }" +
                                "  ]" +
                                "}";
    
    private String schemaResp = "{\n" +
                                "  \"schemas\": [\n" +
                                "    {\n" +
                                "      \"data\": {\n" +
                                "        \"attributes\": {\n" +
                                "          \"$environment\": \"#qa\",\n" +
                                "          \"$owner\": \"#tape\",\n" +
                                "          \"$schema\": \"#perf\",\n" +
                                "          \"device\": \"data.device\",\n" +
                                "          \"driver\": \"data.driver\",\n" +
                                "          \"hostname\": \"metadata.host\",\n" +
                                "          \"type\": \"metadata.type\"\n" +
                                "        },\n" +
                                "        \"filter\": {\n" +
                                "          \"attribute\": {\n" +
                                "            \"type\": \"perf\"\n" +
                                "          }\n" +
                                "        },\n" +
                                "        \"sources\": \"tape_logs\",\n" +
                                "        \"timestamp\": {\n" +
                                "          \"key\": \"data.timestamp\"\n" +
                                "        },\n" +
                                "        \"value\": {\n" +
                                "          \"keys\": {\n" +
                                "            \"rdbcount\": \"data.rdbcount\",\n" +
                                "            \"rdbrate\": \"data.rdbrate\",\n" +
                                "            \"tps\": \"data.tps\",\n" +
                                "            \"wrbcount\": \"data.wrbcount\",\n" +
                                "            \"wrbrate\": \"data.wrbrate\"\n" +
                                "          }\n" +
                                "        }\n" +
                                "      },\n" +
                                "      \"enabled\": true,\n" +
                                "      \"environment\": \"qa\",\n" +
                                "      \"id\": 3,\n" +
                                "      \"name\": \"perf\",\n" +
                                "      \"project\": \"tape\"\n" +
                                "    },\n" +
                                "    {\n" +
                                "      \"data\": {\n" +
                                "        \"attributes\": {\n" +
                                "          \"$environment\": \"#qa\",\n" +
                                "          \"$owner\": \"#tape\",\n" +
                                "          \"$schema\": \"#tapeserverd-count\",\n" +
                                "          \"hostname\": \"metadata.hostname\",\n" +
                                "          \"type\": \"metadata.type\"\n" +
                                "        },\n" +
                                "        \"filter\": {\n" +
                                "          \"attribute\": {\n" +
                                "            \"type\": \"tapeserverd-log-count\"\n" +
                                "          }\n" +
                                "        },\n" +
                                "        \"sources\": \"tape_logs\",\n" +
                                "        \"timestamp\": {\n" +
                                "          \"key\": \"metadata.timestamp\"\n" +
                                "        },\n" +
                                "        \"value\": {\n" +
                                "          \"keys\": {\n" +
                                "            \"count\": \"data.previous_hour_count\"\n" +
                                "          }\n" +
                                "        }\n" +
                                "      },\n" +
                                "      \"enabled\": true,\n" +
                                "      \"environment\": \"qa\",\n" +
                                "      \"id\": 5,\n" +
                                "      \"name\": \"tapeserverd-count\",\n" +
                                "      \"project\": \"tape\"\n" +
                                "    }" +
                                "  ]" +
                                "}";
    
    private String monitoResp = "{\n" +
                                "  \"monitors\": [\n" +
                                "    {\n" +
                                "      \"data\": {\n" +
                                "        \"analysis\": {\n" +
                                "          \"error.lowerbound\": 0,\n" +
                                "          \"type\": \"fixed-threshold\"\n" +
                                "        },\n" +
                                "        \"filter\": {\n" +
                                "          \"attribute\": {\n" +
                                "            \"$defined_metric\": \"perf-missing\",\n" +
                                "            \"$environment\": \"qa\",\n" +
                                "            \"$owner\": \"tape\",\n" +
                                "            \"hostname\": \".*\\\\.cern\\\\.ch\"\n" +
                                "          }\n" +
                                "        },\n" +
                                "        \"notificator\": {\n" +
                                "          \"error-constant\": {\n" +
                                "            \"period\": \"1h\",\n" +
                                "            \"sinks\": \"cern-gni\",\n" +
                                "            \"statuses\": \"ERROR\",\n" +
                                "            \"type\": \"constant\"\n" +
                                "          },\n" +
                                "          \"errors-matt\": {\n" +
                                "            \"filter.attribute.$environment\": \"production\",\n" +
                                "            \"period\": \"1h\",\n" +
                                "            \"silent\": {\n" +
                                "              \"notificator\": {\n" +
                                "                \"sinks\": \"cern-gni\",\n" +
                                "                \"statuses\": \"OK\",\n" +
                                "                \"type\": \"statuses\"\n" +
                                "              },\n" +
                                "              \"period\": \"24h\"\n" +
                                "            },\n" +
                                "            \"sinks\": \"mattermost\",\n" +
                                "            \"statuses\": \"ERROR\",\n" +
                                "            \"tags\": {\n" +
                                "              \"matt-channel\": \"tape-monitoring-mgt\",\n" +
                                "              \"matt-icon\": \"error\",\n" +
                                "              \"matt-text\": \"performance metrics not coming from <metric_attributes:hostname>\"\n" +
                                "            },\n" +
                                "            \"type\": \"constant\"\n" +
                                "          }\n" +
                                "        }\n" +
                                "      },\n" +
                                "      \"enabled\": true,\n" +
                                "      \"environment\": \"qa\",\n" +
                                "      \"id\": 2,\n" +
                                "      \"name\": \"perf-missing\",\n" +
                                "      \"project\": \"tape\"\n" +
                                "    },\n" +
                                "    {\n" +
                                "      \"data\": {\n" +
                                "        \"analysis\": {\n" +
                                "          \"error.upperbound\": 1,\n" +
                                "          \"type\": \"fixed-threshold\",\n" +
                                "          \"warn.lowerbound\": -1\n" +
                                "        },\n" +
                                "        \"filter\": {\n" +
                                "          \"attribute\": {\n" +
                                "            \"$defined_metric\": \"tapeserverd-missing\",\n" +
                                "            \"$environment\": \"qa\",\n" +
                                "            \"$owner\": \"tape\"\n" +
                                "          }\n" +
                                "        },\n" +
                                "        \"notificator\": {\n" +
                                "          \"errors\": {\n" +
                                "            \"filter.attribute.$environment\": \"production\",\n" +
                                "            \"period\": \"3h\",\n" +
                                "            \"sinks\": \"mattermost\",\n" +
                                "            \"statuses\": \"ERROR\",\n" +
                                "            \"tags\": {\n" +
                                "              \"matt-channel\": \"tape-monitoring-mgt\",\n" +
                                "              \"matt-icon\": \"error\",\n" +
                                "              \"matt-text\": \"tapeserverd logs not coming from <metric_attributes:hostname>\"\n" +
                                "            },\n" +
                                "            \"type\": \"constant\"\n" +
                                "          },\n" +
                                "          \"warnings\": {\n" +
                                "            \"sinks\": \"cern-gni\",\n" +
                                "            \"statuses\": \"ERROR WARNING\",\n" +
                                "            \"type\": \"statuses\"\n" +
                                "          }\n" +
                                "        }\n" +
                                "      },\n" +
                                "      \"enabled\": true,\n" +
                                "      \"environment\": \"qa\",\n" +
                                "      \"id\": 3,\n" +
                                "      \"name\": \"tapeserverd-missing\",\n" +
                                "      \"project\": \"tape\"\n" +
                                "    }" +
                                "  ]" +
                                "}";
    
    private static String API_URL = "http://localhost:5000";
    
    @Before
    public void setUp() throws Exception {
        Properties.initCache(null);
        Properties.getCache().reset();
    }

    @Test
    public void loadFromAPI() throws Exception {
        ApiPropertiesSource apiMock = spy(ApiPropertiesSource.class);
        
        JsonParser jparser = new JsonParser();
        doReturn(jparser.parse(schemaResp).getAsJsonObject()).when(apiMock).loadFromUrl(API_URL + "/api/v1/schemas");
        doReturn(jparser.parse(metricResp).getAsJsonObject()).when(apiMock).loadFromUrl(API_URL + "/api/v1/metrics");
        doReturn(jparser.parse(monitoResp).getAsJsonObject()).when(apiMock).loadFromUrl(API_URL + "/api/v1/monitors");
        
        Properties properties = new Properties();
        properties.put("url", API_URL);
        apiMock.config(properties);
        Properties props = apiMock.load();
        
        assertEquals("drive_consec_failed_reqs", props.get("metrics.define.tape_qa_drive_consec_failed_reqs.metrics.attribute.$defined_metric"));
        assertEquals("$owner $environment hostname", props.get("metrics.define.tape_qa_perf-missing.metrics.groupby"));
        
        assertEquals("data.timestamp", props.get("metrics.schema.tape_qa_perf.timestamp.key"));
        assertEquals("#qa", props.get("metrics.schema.tape_qa_tapeserverd-count.attributes.$environment"));
        
        assertEquals("0", props.get("monitor.tape_qa_perf-missing.analysis.error.lowerbound"));
        assertEquals("production", props.get("monitor.tape_qa_tapeserverd-missing.notificator.errors.filter.attribute.$environment"));
    }
}
