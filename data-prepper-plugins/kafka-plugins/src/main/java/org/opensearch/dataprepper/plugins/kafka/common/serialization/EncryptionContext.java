package org.opensearch.dataprepper.plugins.kafka.common.serialization;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;

class EncryptionContext {
    private static final String AES_ALGORITHM = "AES";
    private final Key encryptionKey;

    EncryptionContext(Key encryptionKey) {
        this.encryptionKey = encryptionKey;
    }

    static EncryptionContext fromEncryptionKey(byte[] encryptionKey) {
        SecretKeySpec secretKeySpec = new SecretKeySpec(encryptionKey, AES_ALGORITHM);

        return new EncryptionContext(secretKeySpec);
    }

    Cipher createEncryptionCipher() throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException {
        return createCipher(Cipher.ENCRYPT_MODE);
    }

    Cipher createDecryptionCipher() throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException {
        return createCipher(Cipher.DECRYPT_MODE);
    }

    Key getEncryptionKey() {
        return encryptionKey;
    }

    private Cipher createCipher(int encryptMode) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException {
        Cipher cipher = Cipher.getInstance(AES_ALGORITHM);
        cipher.init(encryptMode, encryptionKey);
        return cipher;
    }
}
