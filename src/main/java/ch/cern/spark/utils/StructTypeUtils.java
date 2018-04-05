package ch.cern.spark.utils;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import ch.cern.utils.Pair;

public class StructTypeUtils {
    
    public static StructType create(Map<String, DataType> keys) {
        StructType struct = new StructType();
        
        for (Entry<String, DataType> key : keys.entrySet())
            if(!key.getKey().contains("."))
                struct = struct.add(key.getKey(), key.getValue());
        
        Map<String, List<Entry<String, DataType>>> groups = keys.entrySet().stream()
                                                                                .filter(key -> key.getKey().contains("."))
                                                                                .collect(Collectors.groupingBy(e -> e.getKey().substring(0, e.getKey().indexOf("."))));
        
        for (Entry<String, List<Entry<String, DataType>>> group : groups.entrySet()) {
            Map<String, DataType> groupKeys = group.getValue().stream()
                                                                .map(e -> new Pair<>(e.getKey().substring(e.getKey().indexOf(".") + 1), e.getValue()))
                                                                .collect(Collectors.toMap(Pair::first, Pair::second));
            
            struct = struct.add(group.getKey(), create(groupKeys));
        }
        
        return struct;
    }

}
