package com.cosmian.cloudproof.spark;

import com.cosmian.jna.covercrypt.CoverCrypt;
import com.cosmian.jna.covercrypt.structs.EncryptedHeader;
import com.cosmian.jna.covercrypt.structs.Policy;
import com.cosmian.utils.CloudproofException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.*;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnPath;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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

        String publicMasterKeyId = fileHadoopConfig.getTrimmed(COVER_CRYPT_PUBLIC_MASTER_KEY);
        if (publicMasterKeyId == null) {
            throw new ParquetCryptoRuntimeException("Undefined CoverCrypt public master key");
        }
        byte[] publicMasterKey = Base64.getDecoder().decode(publicMasterKeyId);

        String policyBytes = fileHadoopConfig.getTrimmed(COVER_CRYPT_POLICY);
        if (policyBytes == null) {
            throw new ParquetCryptoRuntimeException("Undefined CoverCrypt policy");
        }
        Policy policy = new Policy(Base64.getDecoder().decode(policyBytes));


        String partitionsAttributesAsString = fileHadoopConfig.get(COVER_CRYPT_ENCRYPTION_MAPPINGS);
        if (partitionsAttributesAsString == null) {
            throw new ParquetCryptoRuntimeException("Undefined Partitions Attributes");
        }
        EncryptionMappings encryptionMappings = new EncryptionMappings(partitionsAttributesAsString);

        Path folder = tempFilePath.getParent();
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

        String accessPolicy = String.join(" && ", encryptionMappings.getPartitionsAccessPolicies(partitions));

        try {
            EncryptedHeader encryptedHeader = CoverCrypt.encryptHeader(policy, publicMasterKey, accessPolicy);
            byte[] footerKeyBytes = encryptedHeader.getSymmetricKey();
            byte[] footerKeyMetadata = encryptedHeader.getEncryptedHeaderBytes();

            Map<ColumnPath, ColumnEncryptionProperties> encryptedColumns = new HashMap<>();
            for (Map.Entry<String, String> columnMapping : encryptionMappings.columnsMapping.entrySet()) {
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

            ParquetCipher cipher = ParquetCipher.AES_GCM_V1;
            FileEncryptionProperties.Builder fileEncryptionProperties = FileEncryptionProperties.builder(footerKeyBytes)
                    .withFooterKeyMetadata(footerKeyMetadata)
                    .withAlgorithm(cipher);

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
}
