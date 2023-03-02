package com.cosmian.cloudproof.spark;

import com.cosmian.jna.covercrypt.CoverCrypt;
import com.cosmian.jna.covercrypt.structs.DecryptedHeader;
import com.cosmian.jna.covercrypt.structs.EncryptedHeader;
import com.cosmian.jna.covercrypt.structs.Policy;
import com.cosmian.utils.CloudproofException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.*;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

public class CoverCryptCryptoFactory implements EncryptionPropertiesFactory, DecryptionPropertiesFactory {
    public static String COVER_CRYPT_PUBLIC_MASTER_KEY = "parquet.encryption.cover_crypt.public_master_key";

    public static String COVER_CRYPT_DECRYPTION_KEY = "parquet.encryption.cover_crypt.decryption_key";
    public static String COVER_CRYPT_POLICY = "parquet.encryption.cover_crypt.policy";
    public static String COVER_CRYPT_PARTITIONS_ATTRIBUTES = "parquet.encryption.cover_crypt.partitions_attributes";
    public static String COVER_CRYPT_COLUMNS_ATTRIBUTES = "parquet.encryption.cover_crypt.columns_attributes";


    @Override
    public FileDecryptionProperties getFileDecryptionProperties(Configuration hadoopConfig, Path filePath) throws ParquetCryptoRuntimeException {
        String decryptionKeyId = hadoopConfig.getTrimmed(COVER_CRYPT_DECRYPTION_KEY);
        if (decryptionKeyId == null) {
            throw new ParquetCryptoRuntimeException("Undefined CoverCrypt decryption key");
        }
        byte[] decryptionKeyBytes = Base64.getDecoder().decode(decryptionKeyId);

        return FileDecryptionProperties.builder()
                .withKeyRetriever(new DecryptionKeyRetriever() {
                    @Override
                    public byte[] getKey(byte[] keyMetaData)
                            throws KeyAccessDeniedException, ParquetCryptoRuntimeException {
                        try {
                            return CoverCrypt.decryptHeader(decryptionKeyBytes, keyMetaData, Optional.empty()).getSymmetricKey();
                        } catch (CloudproofException e) {
                            throw new ParquetCryptoRuntimeException("Cannot decrypt with CoverCrypt", e);
                        }
                    }
                })
                .withPlaintextFilesAllowed()
                .build();
    }

    @Override
    public FileEncryptionProperties getFileEncryptionProperties(Configuration fileHadoopConfig, Path tempFilePath, WriteSupport.WriteContext fileWriteContext) throws ParquetCryptoRuntimeException {
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


        String partitionsAttributesAsString = fileHadoopConfig.get(COVER_CRYPT_PARTITIONS_ATTRIBUTES);
        if (partitionsAttributesAsString == null) {
            throw new ParquetCryptoRuntimeException("Undefined Partitions Attributes");
        }
        PartitionsAttributes partitionsAttributes = new PartitionsAttributes(partitionsAttributesAsString);

        Path partitions = tempFilePath.getParent();
        List<String> policies = new ArrayList<>();
        while(partitions != null){
            String name = partitions.getName();
            String[] info = name.split("=", 2);

            if (info.length != 2 || info[1].isEmpty()) {
                break;
            }

            System.out.println("- " + info[0] + " ||| " + info[1]);

            {
                String accessPolicy = partitionsAttributes.partitionsValuesMapping.get(info[0] + "=" + info[1]);
                if (accessPolicy != null) {
                    policies.add(accessPolicy);
                    partitions = partitions.getParent();
                    continue;
                }
            }

            {
                String accessPolicy = partitionsAttributes.partitionsDefaultMapping.get(info[0]);
                if (accessPolicy != null) {
                    policies.add(accessPolicy);
                    partitions = partitions.getParent();
                    continue;
                }
            }

            {
                String accessPolicyAxis = partitionsAttributes.partitionsDirectMapping.get(info[0]);
                if (accessPolicyAxis != null) {
                    policies.add(accessPolicyAxis + "::" + info[1]);
                    partitions = partitions.getParent();
                    continue;
                }
            }

            partitions = partitions.getParent();
        }

        String accessPolicy = String.join(" && ", policies);

        try {
            EncryptedHeader encryptedHeader = CoverCrypt.encryptHeader(policy, publicMasterKey, accessPolicy);

            byte[] footerKeyBytes = encryptedHeader.getSymmetricKey();
            byte[] footerKeyMetadata = encryptedHeader.getEncryptedHeaderBytes();

            ParquetCipher cipher = ParquetCipher.AES_GCM_V1;
            return FileEncryptionProperties.builder(footerKeyBytes)
                    .withFooterKeyMetadata(footerKeyMetadata)
                    .withAlgorithm(cipher)
                    .build();
        } catch (CloudproofException e) {
            throw new ParquetCryptoRuntimeException("Cannot encrypt with CoverCrypt", e);
        }
    }
}
