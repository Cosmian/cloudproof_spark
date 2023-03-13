package com.cosmian.cloudproof.spark;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.ColumnEncryptionProperties;
import org.apache.parquet.crypto.DecryptionPropertiesFactory;
import org.apache.parquet.crypto.EncryptionPropertiesFactory;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.KeyAccessDeniedException;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnPath;

import com.cosmian.jna.covercrypt.CoverCrypt;
import com.cosmian.jna.covercrypt.structs.EncryptedHeader;
import com.cosmian.jna.covercrypt.structs.Policy;
import com.cosmian.utils.CloudproofException;

public class CoverCryptCryptoFactory implements EncryptionPropertiesFactory, DecryptionPropertiesFactory {
    public static String COVER_CRYPT_PUBLIC_MASTER_KEY = "parquet.encryption.cover_crypt.public_master_key";
    public static String COVER_CRYPT_DECRYPTION_KEY = "parquet.encryption.cover_crypt.decryption_key";
    public static String COVER_CRYPT_POLICY = "parquet.encryption.cover_crypt.policy";
    public static String COVER_CRYPT_ENCRYPTION_MAPPINGS = "parquet.encryption.cover_crypt.encryption_mappings";

    public static AtomicInteger files = new AtomicInteger();
    public static AtomicLong timings = new AtomicLong();

    @Override
    public FileDecryptionProperties getFileDecryptionProperties(Configuration hadoopConfig, Path filePath) throws ParquetCryptoRuntimeException {
        String decryptionKeyId = hadoopConfig.getTrimmed(COVER_CRYPT_DECRYPTION_KEY);
        if (decryptionKeyId == null) {
            throw new ParquetCryptoRuntimeException("Undefined CoverCrypt decryption key");
        }
        byte[] decryptionKeyBytes = Base64.getDecoder().decode(decryptionKeyId);

        return FileDecryptionProperties.builder()
                .withKeyRetriever(keyMetaData -> {
                    try {
                        return CoverCrypt.decryptHeader(decryptionKeyBytes, keyMetaData, Optional.empty()).getSymmetricKey();
                    } catch (CloudproofException e) {
                        throw new KeyAccessDeniedException("Cannot decrypt with CoverCrypt", e);
                    }
                })
                .withPlaintextFilesAllowed()
                .build();
    }

    @Override
    public FileEncryptionProperties getFileEncryptionProperties(Configuration fileHadoopConfig, Path tempFilePath, WriteSupport.WriteContext fileWriteContext) throws ParquetCryptoRuntimeException {
        long startTime = System.nanoTime();
        files.incrementAndGet();

        // Fetch public master key
        String publicMasterKeyId = fileHadoopConfig.getTrimmed(COVER_CRYPT_PUBLIC_MASTER_KEY);
        if (publicMasterKeyId == null) {
            throw new ParquetCryptoRuntimeException("Undefined CoverCrypt public master key");
        }
        byte[] publicMasterKey = Base64.getDecoder().decode(publicMasterKeyId);

        // Fetch cover crypt policy
        String policyBytes = fileHadoopConfig.getTrimmed(COVER_CRYPT_POLICY);
        if (policyBytes == null) {
            throw new ParquetCryptoRuntimeException("Undefined CoverCrypt policy");
        }
        Policy policy = new Policy(Base64.getDecoder().decode(policyBytes));

        // Fetch encryption mappings from partition/columns to access policies
        String partitionsAttributesAsString = fileHadoopConfig.get(COVER_CRYPT_ENCRYPTION_MAPPINGS);
        if (partitionsAttributesAsString == null) {
            throw new ParquetCryptoRuntimeException("Undefined Partitions Attributes");
        }
        EncryptionMappings encryptionMappings = new EncryptionMappings(partitionsAttributesAsString);

        // This file belongs to specific partitions, from these partitions we can build an access policy.
        List<EncryptionMappings.Partition> partitions = discoverPartitions(tempFilePath);
        String accessPolicy = String.join(" && ", encryptionMappings.getPartitionsAccessPolicies(partitions));

        try {
            // Get the symmetric key for this specific access policy. The encrypted header bytes can only be decrypted
            // by a user decryption key with the correct policy.
            EncryptedHeader encryptedHeader = CoverCrypt.encryptHeader(policy, publicMasterKey, accessPolicy);
            byte[] footerKeyBytes = encryptedHeader.getSymmetricKey();
            byte[] footerKeyMetadata = encryptedHeader.getEncryptedHeaderBytes();

            FileEncryptionProperties.Builder fileEncryptionProperties = FileEncryptionProperties
                    .builder(footerKeyBytes)
                    .withFooterKeyMetadata(footerKeyMetadata)
                    .withAlgorithm(ParquetCipher.AES_GCM_V1);

            // Do the same for each column
            Map<ColumnPath, ColumnEncryptionProperties> encryptedColumns = new HashMap<>();
            for (Map.Entry<String, String> columnMapping : encryptionMappings.getColumnsMapping().entrySet()) {
                EncryptedHeader columnHeader = CoverCrypt.encryptHeader(policy, publicMasterKey, columnMapping.getValue());
                byte[] columnKeyBytes = columnHeader.getSymmetricKey();
                byte[] columnKeyMetadata = columnHeader.getEncryptedHeaderBytes();
                encryptedColumns.put(
                        ColumnPath.fromDotString(columnMapping.getKey()),
                        ColumnEncryptionProperties.builder(columnMapping.getKey())
                                .withKey(columnKeyBytes)
                                .withKeyMetaData(columnKeyMetadata).build()
                );
            }

            if (!encryptedColumns.isEmpty()) {
                fileEncryptionProperties.withEncryptedColumns(encryptedColumns);
            }

            long endTime = System.nanoTime();
            timings.addAndGet(endTime - startTime);

            return fileEncryptionProperties.build();
        } catch (CloudproofException e) {
            throw new ParquetCryptoRuntimeException("Cannot encrypt with CoverCrypt", e);
        }
    }

    /**
     * Spark/Parquet doesn't provide a simple way to find in which partition a specific file
     * is built. We need to parse the folder structure to discover the partitions.
     * This method is working with a local filesystem but may be broken if the partitions doesn't
     * appear inside the path like: "/home/…/xxx.parquet/Country=France/Size=Small/part-00000-7b1ec238-e9d7-4ab3-a7da-6d6db3f30c35.c000.snappy.parquet
     * or if the folder name follow a different format.
     *
     * @param path Parquet filepath
     * @return the list of partition for this specific file
     */
    private List<EncryptionMappings.Partition> discoverPartitions(Path path) {
        Path folder = path.getParent();
        List<EncryptionMappings.Partition> partitions = new ArrayList<>();
        while(folder != null){
            String folderName = folder.getName();
            String[] info = folderName.split("=");

            if (info.length != 2 || info[1].isEmpty()) {
                break;
            }

            partitions.add(new EncryptionMappings.Partition(info[0], info[1]));
            folder = folder.getParent();
        }

        return partitions;
    }
}
