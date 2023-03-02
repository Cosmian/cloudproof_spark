package com.cosmian.cloudproof.spark;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PartitionsAttributes {
    Map<String, String> partitionsValuesMapping = new HashMap<>();
    Map<String, String> partitionsDefaultMapping = new HashMap<>();
    Map<String, String> partitionsDirectMapping = new HashMap<>();

    public PartitionsAttributes() {}

    public PartitionsAttributes(String config) {
        String[] blocks = config.split("\n\n", 3);

        if (blocks.length != 3) {
            throw new RuntimeException("Config '" + config + "' is invalid (should contains 3 blocks, " + blocks.length + "received)");
        }

        partitionsValuesMapping = deserializeMap(blocks[0]);
        partitionsDefaultMapping = deserializeMap(blocks[1]);
        partitionsDirectMapping = deserializeMap(blocks[2]);
    }

    public PartitionsAttributes addPartitionValueMapping(String partition, String accessPolicy) {
        partitionsValuesMapping.put(partition, accessPolicy);
        return this;
    }

    public PartitionsAttributes addPartitionDefaultMapping(String partitionKey, String accessPolicy) {
        partitionsDefaultMapping.put(partitionKey, accessPolicy);
        return this;
    }


    public PartitionsAttributes addPartitionDirectMapping(String partitionKey, String accessPolicyAxis) {
        partitionsDirectMapping.put(partitionKey, accessPolicyAxis);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        serializeMap(builder, partitionsValuesMapping);
        builder.append("\n\n");
        serializeMap(builder, partitionsDefaultMapping);
        builder.append("\n\n");
        serializeMap(builder, partitionsDirectMapping);

        return builder.toString();
    }

    private static void serializeMap(StringBuilder builder, Map<String, String> map) {
        boolean first = true;

        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (! first) {
                builder.append('\n');
            }
            first = false;

            builder.append(entry.getKey());
            builder.append('\t');
            builder.append(entry.getValue());
        }
    }

    private static Map<String, String> deserializeMap(String lines) {
        Map<String, String> map = new HashMap<>();

        if (lines.isEmpty()) {
            return map;
        }

        for (String line : lines.split("\n")) {
            String[] info = line.split("\t");

            if (info.length != 2) {
                throw new RuntimeException("Mapping '" + line + "' is invalid (should contains 2 \\t blocks) inside " + lines);
            }

            map.put(info[0], info[1]);
        }

        return map;
    }
}
