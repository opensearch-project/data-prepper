/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.obfuscation.action;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.plugins.processor.obfuscation.ObfuscationProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



@DataPrepperPlugin(name = "hash", pluginType = ObfuscationAction.class, pluginConfigurationType = OneWayHashActionConfig.class)
public class OneWayHashAction implements ObfuscationAction {
    

    private MessageDigest messageDigest = null;
    private byte[] salt;
    private static final Logger LOG = LoggerFactory.getLogger(ObfuscationProcessor.class);

    @DataPrepperPluginConstructor
    public OneWayHashAction(final OneWayHashActionConfig config) {
        
        this.salt = config.getSalt();
        try{
            messageDigest = MessageDigest.getInstance(config.getHashFormat());            
        } catch (NoSuchAlgorithmException noSuchAlgorithmException){
            LOG.error("There was an exception while processing Event [{}]", noSuchAlgorithmException);
        }
    }

    @Override
    public String obfuscate(String source, List<Pattern> patterns) {
        
        if (patterns == null || patterns.size() == 0) {
            // no pattern to match, replace the whole string
            return oneWayHashString(source);
        }
        
        String replacementString = source;

        for (Pattern pattern : patterns) {
            Matcher matcher = Pattern.compile(pattern.pattern()).matcher(replacementString);
            StringBuffer stringBuffer = new StringBuffer();
            while (matcher.find()) {
                System.out.println("SOURCE: "+source);    
                System.out.println("START: "+ matcher.start());

                System.out.println("OFFSET: "+ matcher.end());

                String stringToHash = source.substring(matcher.start(),matcher.end());

                matcher.appendReplacement(stringBuffer, oneWayHashString(stringToHash));
            }
            matcher.appendTail(stringBuffer);
            replacementString = stringBuffer.toString();
        }
        return replacementString;
        
        
    }

    private String oneWayHashString(String source){
        String oneWayHashedSource = "";

        try {
            MessageDigest messageDigestClone = (MessageDigest) messageDigest.clone();

            messageDigestClone.update(this.salt);
            byte[] bytes = messageDigestClone.digest(source.getBytes(StandardCharsets.UTF_8));
            
            oneWayHashedSource =  Base64.getEncoder().encodeToString(bytes);

        } catch (CloneNotSupportedException cloneNotSupportedException){
            LOG.error("There was an exception while processing Event [{}]", cloneNotSupportedException);
        } 
    
        return oneWayHashedSource;
    }


}
