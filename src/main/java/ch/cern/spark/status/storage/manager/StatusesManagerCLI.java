package ch.cern.spark.status.storage.manager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import ch.cern.exdemon.Driver;
import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.ComponentBuildResult;
import ch.cern.exdemon.components.ComponentTypes;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.SparkConf;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.JSONStatusSerializer;
import ch.cern.spark.status.storage.JavaStatusSerializer;
import ch.cern.spark.status.storage.StatusSerializer;
import ch.cern.spark.status.storage.StatusesStorage;
import ch.cern.utils.TimeUtils;
import scala.Tuple2;

public class StatusesManagerCLI {
    
    private StatusesStorage storage;
    private JavaSparkContext context;
        
    private String filter_by_id;
    private String filter_key_regex;
    private String filter_by_fqcn;
    private long filter_value_size;
    
    private JSONStatusSerializer json = new JSONStatusSerializer();
    
    private StatusSerializer serializer;
    private String saving_path;
    
    private String statuses_removal_socket_host;
    private int statuses_removal_socket_port;
    
    private Duration expired_period;
    
    private boolean remove_all;
    private boolean remove_kafka;
    
    private static Scanner STDIN = new Scanner(System.in);
    
    public StatusesManagerCLI() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("KafkaStatusesManagement");
        sparkConf.setMaster("local[2]");
        sparkConf.set("spark.driver.host", "localhost");
        sparkConf.set("spark.driver.allowMultipleContexts", "true");
        
        context = new JavaSparkContext(sparkConf);
    }
    
    public static void main(String[] args) throws ConfigurationException, IOException {
        CommandLine cmd = parseCommand(args);
        if(cmd == null)
            return;
        
        Properties properties = Properties.fromFile(cmd.getOptionValue("conf"));
        
        StatusesManagerCLI manager = new StatusesManagerCLI();
        manager.config(properties, cmd);
        
        JavaPairRDD<StatusKey, StatusValue> filteredStatuses = manager.loadAndFilter();
        
        long size = filteredStatuses.count();
        
        StatusKey key = null;
        StatusValue value = null;
        if(size > 1) {
            Map<Integer, StatusKey> indexedKeys = getInxedKeys(filteredStatuses);
            manager.printKeys(indexedKeys);
            
            if(manager.shouldRemoveAll()) {
                manager.remove(indexedKeys.values().toArray(new StatusKey[0]));
                
                System.out.println("All filtered statuses removed.");
                return;
            }
            
            int index = askForIndex();
            
            key = indexedKeys.get(index);
            List<StatusValue> values = filteredStatuses.lookup(key);
            if(key == null || values.size() < 1) {
                System.out.println("There is no value for this key.");
                System.exit(1);
            }
            
            value = values.get(0);
        }else if(size == 1){
            Tuple2<StatusKey, StatusValue> tuple = filteredStatuses.collect().get(0);
            
            key = tuple._1;
            value = tuple._2;
        }else {
            System.out.println("No key found with that criteria.");
            System.exit(0);
        }

        manager.printDetailedInfo(key, value);
        manager.save(key, value);
        
        if(askRemove())
            manager.remove(key);
        
        STDIN.close();
    }

    private boolean shouldRemoveAll() {
        return remove_all;
    }

    private void remove(StatusKey... statusKeys) throws UnknownHostException, IOException, ConfigurationException {
        if(remove_kafka) {
            storage.remove(context.parallelize(Arrays.asList(statusKeys)));
            
            return;
        }
            
        String actualHostname = InetAddress.getLocalHost().getHostName();
        String configuredName = InetAddress.getByName(statuses_removal_socket_host).getHostName();
        
        if(!actualHostname.equals(configuredName))
            throw new ConfigurationException(null, "job is listening on " + configuredName + ", but this command is being run on " + actualHostname);
        
        @SuppressWarnings("resource")
        Socket socket = new ServerSocket(statuses_removal_socket_port).accept();
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
        
        for (StatusKey statusKey : statusKeys) {
            String jsonString = new String(json.fromKey(statusKey));
            
            System.out.println();
            System.out.println("Sending key for removal: " + jsonString);
            
            writer.println(jsonString);
            
            String answer = reader.readLine();
            
            System.out.println("Removed: " + answer);
        }
        
        socket.close();
    }

    private static boolean askRemove() {        
        System.out.println();
        System.out.println("Remove status? (Y, n or exit): ");
        String answer = STDIN.nextLine();
        if(answer == null || answer.equals("exit"))
            System.exit(0);
        
        boolean remove = false;
        if(answer != null && answer.equals("Y"))
            remove = true;
        
        return remove;
    }

    private void save(StatusKey key, StatusValue value) throws IOException {
        if(saving_path == null)
            return;
        
        PrintWriter writer = new PrintWriter(saving_path + ".key", "UTF-8");
        writer.println(new String(json.fromKey(key)));
        writer.close();
        
        writer = new PrintWriter(saving_path + ".value", "UTF-8");
        writer.println(new String(json.fromValue(value)));
        writer.close();
        
        System.out.println();
        System.out.println("JSON document saved at: " + saving_path + "(.key, .value)");
    }

    private void printDetailedInfo(StatusKey key, StatusValue value) throws IOException {
        if(serializer == null)
            return;
        
        System.out.println();
        System.out.println("Detailed information:");
        System.out.println("- Key: " + new String(serializer.fromKey(key)));
        System.out.println("- Updated value at: " + ZonedDateTime.ofInstant(Instant.ofEpochMilli(value.getStatus_update_time()), ZoneOffset.systemDefault()));
        
        String valueString = new String(serializer.fromValue(value));
        if(valueString.length() > 1001)
            valueString = valueString.substring(0, 1000) + "...";
        
        System.out.println("- Value: " + valueString);
    }

    private static int askForIndex() {
        System.out.println();
        
        System.out.println("Index number for detailed information (or exit): ");
        String indexString = STDIN.nextLine();
        if(indexString == null || indexString.equals("exit"))
            System.exit(0);
        
        int index = -1;
        try {
            index = Integer.parseInt(indexString);
        }catch(Exception e) {
            System.out.println("Wrong number: " + indexString);
            
            System.exit(1);
        }
        
        return index;
    }

    private static Map<Integer, StatusKey> getInxedKeys(JavaPairRDD<StatusKey, StatusValue> filteredStatuses) {
        List<StatusKey> keys = filteredStatuses.map(tuple -> tuple._1).collect();
        
        Map<Integer, StatusKey> index = new HashMap<>();
        int i = 0;
        for (StatusKey statusKey : keys)
            index.put(i++, statusKey);
            
        return index;
    }

    private void printKeys(Map<Integer, StatusKey> indexedKeys) throws IOException, ConfigurationException {
        if(serializer == null)
            throw new ConfigurationException(null, "several keys has been found but not print option has been specified, so they cannot be listed.");
        
        System.out.println("List of found keys:");
        
        if(serializer instanceof JavaStatusSerializer) {
            for (Map.Entry<Integer, StatusKey> key : indexedKeys.entrySet())
                System.out.println(key.getKey() + ":\t" + key.getValue().toString());
        }else {
            for (Map.Entry<Integer, StatusKey> key : indexedKeys.entrySet())
                System.out.println(key.getKey() + ":\t" + new String(serializer.fromKey(key.getValue())));
        }
    }

    public JavaPairRDD<StatusKey, StatusValue> loadAndFilter() throws IOException, ConfigurationException {
        JavaRDD<Tuple2<StatusKey, StatusValue>> statuses = storage.load(context);
        
        if(filter_by_id != null)
            statuses = statuses.filter(new IDStatusKeyFilter(filter_by_id));
        
        if(filter_key_regex != null)
            statuses = statuses.filter(new ToStringPatternStatusKeyFilter(filter_key_regex));
        
        if(filter_by_fqcn != null)
            statuses = statuses.filter(new ClassNameStatusKeyFilter(filter_by_fqcn));
        
        if(filter_value_size != Long.MAX_VALUE)
            statuses = statuses.filter(new ValueSizeFilter(filter_value_size, serializer));
        
        if(expired_period != null)
            statuses = statuses.filter(new ExpireStatusValueFilter(expired_period));
        
        return statuses.mapToPair(tuple -> tuple);
    }

    public static CommandLine parseCommand(String[] args) {
        Options options = new Options();
        
        Option brokers = new Option("c", "conf", true, "path to configuration file");
        brokers.setRequired(true);
        options.addOption(brokers);
        
        options.addOption(new Option("id", "id", true, "filter by status key id"));
        options.addOption(new Option("kr", "key-regex", true, "filter by regex that matches key.toString"));
        options.addOption(new Option("n", "fqcn", true, "filter by FQCN or alias"));
        options.addOption(new Option("e", "expired", true, "filter by expired values, expiration period like 1m, 3h, 5d"));
        options.addOption(new Option("vs", "value-size", true, "filter by minimum value size in bytes"));
        
        options.addOption(new Option("p", "print", true, "print mode: java or json"));
        
        options.addOption(new Option("s", "save", true, "path to write result as JSON"));
        
        options.addOption(new Option("r", "remove", false, "remove all statuses filtered"));
        
        options.addOption(new Option("rk", "remove-kafka", false, "remove from Kafka"));
        
        CommandLineParser parser = new BasicParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLine cmd = parser.parse(options, args);
            
            return cmd;
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("spark-statuses-manager", options);

            return null;
        }
    }

    protected void config(Properties properties, CommandLine cmd) throws ConfigurationException  {
        ComponentBuildResult<StatusesStorage> storageBuildResult = ComponentTypes.build(Type.STATUS_STORAGE, properties.getSubset(StatusesStorage.STATUS_STORAGE_PARAM));
        storageBuildResult.throwExceptionsIfPresent();
        storage = storageBuildResult.getComponent().get();
        
        String removalSocket = properties.getProperty(Driver.STATUSES_REMOVAL_SOCKET_PARAM);
        if(removalSocket != null) {
            String[] host_port = removalSocket.trim().split(":");
            
            statuses_removal_socket_host = host_port[0];
            statuses_removal_socket_port = Integer.parseInt(host_port[1]);
        }
        
        filter_by_id = cmd.getOptionValue("id");
        filter_key_regex = cmd.getOptionValue("key-regex");
        filter_by_fqcn = cmd.getOptionValue("fqcn");
        String filter_value_sizeString = cmd.getOptionValue("value-size");
        if(filter_value_sizeString != null)
            filter_value_size = Long.parseLong(filter_value_sizeString);
        else
            filter_value_size = Long.MAX_VALUE;
        
        if(cmd.getOptionValue("print") == null)
            serializer = null;
        else if(cmd.getOptionValue("print").equals("java"))
            serializer = new JavaStatusSerializer();
        else if(cmd.getOptionValue("print").equals("json"))
            serializer = new JSONStatusSerializer();
        else
            throw new ConfigurationException(null, "print option " + cmd.getOptionValue("print") + " is not available");
        
        saving_path = cmd.getOptionValue("save");
        
        remove_all = cmd.hasOption("remove");
        
        remove_kafka = cmd.hasOption("remove-kafka");
        
        String expiredString = cmd.getOptionValue("expired");
        if(expiredString != null)
            expired_period = TimeUtils.parsePeriod(expiredString);
    }
    
    public void close(){
        if(context != null)
            context.stop();
        context = null;
    }
    
}
