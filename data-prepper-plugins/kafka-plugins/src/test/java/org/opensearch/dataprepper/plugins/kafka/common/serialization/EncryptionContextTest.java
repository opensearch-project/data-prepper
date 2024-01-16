package org.opensearch.dataprepper.plugins.kafka.common.serialization;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class EncryptionContextTest {
    private SecretKey aesKey;

    @BeforeEach
    void setUp() throws NoSuchAlgorithmException {
        aesKey = createAesKey();
    }

    private EncryptionContext createObjectUnderTest() {
        return new EncryptionContext(aesKey);
    }

    @Test
    void encryption_and_decryption_are_symmetric() throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException {
        EncryptionContext objectUnderTest = createObjectUnderTest();

        Cipher encryptionCipher = objectUnderTest.createEncryptionCipher();

        Cipher decryptionCipher = objectUnderTest.createDecryptionCipher();

        byte[] inputBytes = UUID.randomUUID().toString().getBytes();

        byte[] encryptedBytes = encryptionCipher.doFinal(inputBytes);

        byte[] decryptedBytes = decryptionCipher.doFinal(encryptedBytes);

        assertThat(decryptedBytes, equalTo(inputBytes));
    }

    @Test
    void fromEncryptionKey_includes_correct_Key() {
        byte[] key = aesKey.getEncoded();

        EncryptionContext encryptionContext = EncryptionContext.fromEncryptionKey(key);

        assertThat(encryptionContext.getEncryptionKey(), notNullValue());
        assertThat(encryptionContext.getEncryptionKey().getEncoded(), equalTo(aesKey.getEncoded()));
        assertThat(encryptionContext.getEncryptionKey().getAlgorithm(), equalTo("AES"));
    }

    private static SecretKey createAesKey() throws NoSuchAlgorithmException {
        KeyGenerator aesKeyGenerator = KeyGenerator.getInstance("AES");
        aesKeyGenerator.init(256);
        return aesKeyGenerator.generateKey();
    }
}