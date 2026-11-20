/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.obfuscation.action;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import org.opensearch.dataprepper.model.pattern.Matcher;
import org.opensearch.dataprepper.model.pattern.Pattern;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.processor.obfuscation.ObfuscationProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.opensearch.dataprepper.model.event.EventKey;

@DataPrepperPlugin(name = "hash", pluginType = ObfuscationAction.class, pluginConfigurationType = OneWayHashActionConfig.class)
public class OneWayHashAction implements ObfuscationAction {
    

    private final  MessageDigest messageDigest;
    private final byte[] salt;
    private EventKey saltKey;
    private static final Logger LOG = LoggerFactory.getLogger(ObfuscationProcessor.class);

    @DataPrepperPluginConstructor
    public OneWayHashAction(final OneWayHashActionConfig config) {
        
        this.saltKey = config.getSaltKey();        

        if (config.getSalt() == null || config.getSalt().isEmpty() ) {
            this.salt = generateSalt();
        } else {
            this.salt = config.getSalt().getBytes(StandardCharsets.UTF_8);
        }

        try {
            messageDigest = MessageDigest.getInstance(config.getFormat());            
        } catch (NoSuchAlgorithmException noSuchAlgorithmException){
            LOG.error("The hash format provided ({}) is not a known algorithm [{}]", config.getFormat(), noSuchAlgorithmException);
            throw new RuntimeException(noSuchAlgorithmException);
        }
    }

    @Override
    public String obfuscate(String source, List<Pattern> patterns, Record<Event> record) {

        byte [] saltToApply = this.salt;
                
        // Resolve salt to compute based on a path provided in the configuration.  
        // For records where path was not found, the salt value defined in the pipeline configuration will be used, if salt value was not configured, one will be generated.        

        if(saltKey != null && saltKey.equals("") == false) {       

            final Event recordEvent = record.getData();                        

            if (recordEvent.containsKey(saltKey)) {

                saltToApply = computeSaltBasedOnKeyValue(recordEvent.get(saltKey, String.class));
            } else {
                LOG.info("Unable to find a key '{}' for using as salt, using default salt pipeline configuration for the record instead", saltKey);
            }         
        }

        if (patterns == null || patterns.size() == 0) {
            // no pattern to match, replace the whole string
            return oneWayHashString(source,saltToApply);
        }
        
        String replacementString = source;   

        for (Pattern pattern : patterns) {
            
            Matcher matcher = Pattern.compile(pattern.pattern()).matcher(replacementString);
            StringBuffer stringBuffer = new StringBuffer();

            while (matcher.find()) {

                String stringToHash = replacementString.substring(matcher.start(),matcher.end());                
                matcher.appendReplacement(stringBuffer, oneWayHashString(stringToHash,saltToApply));                
            }

            matcher.appendTail(stringBuffer);            
            replacementString = stringBuffer.toString();
        }
        return replacementString;
        
        
    }

    private String oneWayHashString(String source, byte[] salt) {

        String oneWayHashedSource = "";

        try {
            MessageDigest messageDigestClone = (MessageDigest) messageDigest.clone();

            messageDigestClone.update(salt);
            byte[] bytes = messageDigestClone.digest(source.getBytes(StandardCharsets.UTF_8));
            
            oneWayHashedSource =  Base64.getEncoder().encodeToString(bytes);

        } catch (CloneNotSupportedException cloneNotSupportedException) {
            LOG.error("There was an exception while processing Event [{}]", cloneNotSupportedException);
            throw new RuntimeException(cloneNotSupportedException);
        } 

        return oneWayHashedSource;
    }

    private byte [] computeSaltBasedOnKeyValue(String saltValue) {

        byte [] value = saltValue.getBytes(StandardCharsets.UTF_8);
        byte [] result = new byte [64];

        Arrays.fill(result, Byte.MIN_VALUE);

        System.arraycopy(value, 0, result, 0, 
            (value.length >= result.length) ? result.length : value.length);

        return result;
    }

    private byte[] generateSalt() {

        byte [] saltBytes = new byte[64];
        SecureRandom secureRandom = new SecureRandom();
        secureRandom.nextBytes(saltBytes);        
        return saltBytes;
    }
}
