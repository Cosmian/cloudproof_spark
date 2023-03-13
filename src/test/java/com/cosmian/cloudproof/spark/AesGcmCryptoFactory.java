package com.cosmian.cloudproof.spark;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.DecryptionPropertiesFactory;
import org.apache.parquet.crypto.EncryptionPropertiesFactory;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.KeyAccessDeniedException;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.hadoop.api.WriteSupport;

import com.cosmian.jna.covercrypt.CoverCrypt;
import com.cosmian.utils.CloudproofException;

public class AesGcmCryptoFactory implements EncryptionPropertiesFactory, DecryptionPropertiesFactory {

    public static AtomicInteger files = new AtomicInteger();
    public static AtomicLong timings = new AtomicLong();

    @Override
    public FileDecryptionProperties getFileDecryptionProperties(Configuration hadoopConfig, Path filePath)
            throws ParquetCryptoRuntimeException {
        byte[] decryptionKeyBytes = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32 };

        return FileDecryptionProperties.builder()
                .withKeyRetriever(keyMetaData -> {
                    try {
                        return CoverCrypt.decryptHeader(decryptionKeyBytes, keyMetaData, Optional.empty())
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
        long startTime = System.nanoTime();
        files.incrementAndGet();

        FileEncryptionProperties.Builder fileEncryptionProperties = FileEncryptionProperties
                .builder(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                        24, 25, 26, 27, 28, 29, 30, 31, 32 })
                .withAlgorithm(ParquetCipher.AES_GCM_V1);

        long endTime = System.nanoTime();
        timings.addAndGet(endTime - startTime);

        return fileEncryptionProperties.build();

    }

}
