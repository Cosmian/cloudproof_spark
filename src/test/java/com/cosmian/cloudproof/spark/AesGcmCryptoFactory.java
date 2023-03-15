package com.cosmian.cloudproof.spark;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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
import com.cosmian.utils.CloudproofException;

public class AesGcmCryptoFactory implements EncryptionPropertiesFactory, DecryptionPropertiesFactory {

    public static String ENCRYPTION_MAPPINGS = "parquet.encryption.aesgcm.encryption_mappings";
    public static byte[] ENCRYPTION_KEY = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
            19, 20,
            21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32 };

    @Override
    public FileDecryptionProperties getFileDecryptionProperties(Configuration hadoopConfig, Path filePath)
            throws ParquetCryptoRuntimeException {
        return FileDecryptionProperties.builder()
                .withKeyRetriever(keyMetaData -> {
                    try {
                        return CoverCrypt.decryptHeader(
                                ENCRYPTION_KEY, keyMetaData, Optional.empty())
                                .getSymmetricKey();
                    } catch (CloudproofException e) {
                        throw new KeyAccessDeniedException("Cannot decrypt with CoverCrypt", e);
                    }
                })
                .withPlaintextFilesAllowed()
                .build();
    }

    @Override
    public FileEncryptionProperties getFileEncryptionProperties(Configuration fileHadoopConfig, Path tempFilePath,
            WriteSupport.WriteContext fileWriteContext) throws ParquetCryptoRuntimeException {
        // Fetch encryption mappings from partition/columns to access policies
        String partitionsAttributesAsString = fileHadoopConfig.get(ENCRYPTION_MAPPINGS);
        if (partitionsAttributesAsString == null) {
            throw new ParquetCryptoRuntimeException("Undefined Partitions Attributes");
        }
        EncryptionMappings encryptionMappings = new EncryptionMappings(partitionsAttributesAsString);

        Map<ColumnPath, ColumnEncryptionProperties> encryptedColumns = new HashMap<>();
        for (Map.Entry<String, String> columnMapping : encryptionMappings.getColumnsMapping().entrySet()) {
            encryptedColumns.put(
                    ColumnPath.fromDotString(columnMapping.getKey()),
                    ColumnEncryptionProperties.builder(columnMapping.getKey())
                            .withKey(
                                    ENCRYPTION_KEY)
                            .build());
            // .withKeyMetaData(columnKeyMetadata).build());
        }

        FileEncryptionProperties.Builder fileEncryptionProperties = FileEncryptionProperties
                .builder(
                        ENCRYPTION_KEY)
                .withAlgorithm(ParquetCipher.AES_GCM_V1);
        if (!encryptedColumns.isEmpty()) {
            fileEncryptionProperties.withEncryptedColumns(encryptedColumns);
        }

        return fileEncryptionProperties.build();
    }
}
