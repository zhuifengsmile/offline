package utils;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by zhuifeng on 2017/6/3.
 */
public class NestedTableAggregator {
    private static final Logger logger = LoggerFactory.getLogger(NestedTableAggregator.class);
    private static ObjectMapper mapper = new ObjectMapper();

    public static void aggregate(Map<String, String> fieldMap) {
        Map<String, Map<String, String>> nestedTables = new HashMap<>();
        Set<String> nestedFields = new HashSet<>();

        for (Map.Entry<String, String> entry : fieldMap.entrySet()) {
            String fieldName = entry.getKey();
            String fieldValue = entry.getValue();
            // family:qualifier:{SOURCE_KEY_VALUE} or family:qualifier
            String[] familyQualifier = fieldName.split(":", 2);

            if (familyQualifier.length < 2) {
                continue;
            }

            String qualifier = familyQualifier[1];
            int idx = qualifier.indexOf(":");
            if (idx < 0) {
                continue;
            }
            nestedFields.add(fieldName);

            String nestedTableName = familyQualifier[0] + ":" + qualifier.substring(0, idx);
            String nestedTableField = qualifier.substring(idx + 1);

            if (!nestedTables.containsKey(nestedTableName)) {
                nestedTables.put(nestedTableName, new HashMap<String, String>());
            }
            nestedTables.get(nestedTableName).put(nestedTableField, fieldValue);
        }

        // remove all nested fields
        fieldMap.keySet().removeAll(nestedFields);

        // add nested table fields
        for (Map.Entry<String, Map<String, String>> entry : nestedTables.entrySet()) {
            fieldMap.put(entry.getKey(), map2jsonstring(entry.getValue()));
        }
    }

    private static String map2jsonstring(Map<String, String> map) {
        try {
            return mapper.writeValueAsString(map);
        } catch (Exception e) {
            logger.debug("failed to generate json string");
        }
        return "";
    }

    private static Map<String, String> json2Map(String json) {
        Map<String, String> map = new HashMap<>();
        try {
            map = mapper.readValue(json, new TypeReference<HashMap<String, String>>() {

            });
        } catch (Exception e) {
            //just discard
            logger.error("failed to generate json string");
        }
        return map;
    }
}

