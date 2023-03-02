package com.cosmian.cloudproof.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.KeyAccessDeniedException;
import org.apache.parquet.crypto.keytools.KmsClient;

public class InMemoryKms implements KmsClient {
    @Override
    public void initialize(Configuration configuration, String kmsInstanceID, String kmsInstanceURL, String accessToken) throws KeyAccessDeniedException {

    }

    @Override
    public String wrapKey(byte[] keyBytes, String masterKeyIdentifier) throws KeyAccessDeniedException {
        System.out.println("wrapKey!!! " + masterKeyIdentifier);
        return "null";
    }

    @Override
    public byte[] unwrapKey(String wrappedKey, String masterKeyIdentifier) throws KeyAccessDeniedException {
        return new byte[0];
    }
}
