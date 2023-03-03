package com.cosmian.cloudproof.spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is a simple data class to format/parse to/from String multiple mappings from partitions/columns to CoverCrypt access policies.
 * Since Hadoop configuration options don't allow anything other than a String, we print all the maps in different blocks divided by \n\n.
 * The elements inside the maps are each on one line, key and values are separated by \t. (This doesn't allow \t nor \n inside access policies or partitions/columns name/values)
 * Right now, the `EncryptionMappings` is hard coded, but we could provide a way to specify a user class here to have more fine grain control over the mappings.
 */
public class EncryptionMappings {
    /**
     * Association between "Country=France" and "Country::France" or
     * "Size=Big" and "Security::Secret".
     */
    private Map<String, String> partitionsValuesMapping = new HashMap<>();

    /**
     * Association between "Country" and "Country::Others". All partitions' values will be encrypted
     * with the same policy.
     */
    private Map<String, String> partitionsDefaultMapping = new HashMap<>();

    /**
     * Association between "Size" and "Security" where all "Size"'s partition values match 1-1 to an attribute
     * inside the "Security" axis. For example "Size::Small" will be encrypted with "Security::Small", etc.
     */
    private Map<String, String> partitionsDirectMapping = new HashMap<>();

    /**
     * Association between "Name" and "Security::Secret". The column "Name" will be encrypted with the access policy
     * "Security::Secret".
     * If the column is not present inside this mappings map, the column will remain unencrypted (the file will be encrypted
     * following the partitions' mappings). We may provide a way in the future to add a default policy for the remaining columns.
     */
    private Map<String, String> columnsMapping = new HashMap<>();

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

    public EncryptionMappings addPartitionDefaultMapping(String partitionKey, String accessPolicy) throws PartitionAlreadyExistsInAnotherMappingException {
        if (partitionsDirectMapping.containsKey(partitionKey)) {
            throw new PartitionAlreadyExistsInAnotherMappingException("Cannot a default mapping to partition " + partitionKey + " because this partition already exists inside the direct mapping.");
        }

        partitionsDefaultMapping.put(partitionKey, accessPolicy);
        return this;
    }


    public EncryptionMappings addPartitionDirectMapping(String partitionKey, String accessPolicyAxis) throws PartitionAlreadyExistsInAnotherMappingException {
        if (partitionsDefaultMapping.containsKey(partitionKey)) {
            throw new PartitionAlreadyExistsInAnotherMappingException("Cannot a direct mapping to partition " + partitionKey + " because this partition already exists inside the default mapping.");
        }

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

    public Map<String, String> getColumnsMapping() {
        return columnsMapping;
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

    static public class PartitionAlreadyExistsInAnotherMappingException extends Exception {
        PartitionAlreadyExistsInAnotherMappingException(String message) {
            super(message);
        }
    }
}
