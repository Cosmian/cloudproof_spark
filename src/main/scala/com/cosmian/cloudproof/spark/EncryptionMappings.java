package com.cosmian.cloudproof.spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EncryptionMappings {
    Map<String, String> partitionsValuesMapping = new HashMap<>();
    Map<String, String> partitionsDefaultMapping = new HashMap<>();
    Map<String, String> partitionsDirectMapping = new HashMap<>();
    Map<String, String> columnsMapping = new HashMap<>();

    public EncryptionMappings() {}

    public EncryptionMappings(String config) {
        String[] blocks = config.split("\n\n", 4);

        if (blocks.length != 4) {
            throw new RuntimeException("Config '" + config + "' is invalid (should contains 3 blocks, " + blocks.length + "received)");
        }

        partitionsValuesMapping = deserializeMap(blocks[0]);
        partitionsDefaultMapping = deserializeMap(blocks[1]);
        partitionsDirectMapping = deserializeMap(blocks[2]);
        columnsMapping = deserializeMap(blocks[3]);
    }

    public EncryptionMappings addPartitionValueMapping(String partition, String accessPolicy) {
        partitionsValuesMapping.put(partition, accessPolicy);
        return this;
    }

    public EncryptionMappings addPartitionDefaultMapping(String partitionKey, String accessPolicy) {
        partitionsDefaultMapping.put(partitionKey, accessPolicy);
        return this;
    }


    public EncryptionMappings addPartitionDirectMapping(String partitionKey, String accessPolicyAxis) {
        partitionsDirectMapping.put(partitionKey, accessPolicyAxis);
        return this;
    }


    public EncryptionMappings addColumnMapping(String columnName, String accessPolicy) {
        columnsMapping.put(columnName, accessPolicy);
        return this;
    }

    public List<String> getPartitionsAccessPolicies(List<Partition> partitions) {
        List<String> policies = new ArrayList<>();

        for (Partition partition : partitions) {
            {
                String accessPolicy = partitionsValuesMapping.get(partition.name + "=" + partition.value);
                if (accessPolicy != null) {
                    policies.add(accessPolicy);
                    continue;
                }
            }

            {
                String accessPolicy = partitionsDefaultMapping.get(partition.name);
                if (accessPolicy != null) {
                    policies.add(accessPolicy);
                    continue;
                }
            }

            {
                String accessPolicyAxis = partitionsDirectMapping.get(partition.name);
                if (accessPolicyAxis != null) {
                    policies.add(accessPolicyAxis + "::" + partition.value);
                }
            }
        }

        return policies;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        serializeMap(builder, partitionsValuesMapping);
        builder.append("\n\n");
        serializeMap(builder, partitionsDefaultMapping);
        builder.append("\n\n");
        serializeMap(builder, partitionsDirectMapping);
        builder.append("\n\n");
        serializeMap(builder, columnsMapping);

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

    static public class Partition {
        public String name;
        public String value;

        public Partition(String name, String value) {
            this.name = name;
            this.value = value;
        }
    }
}
