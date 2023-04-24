package org.opensearch.dataprepper.plugins.source;

import java.net.MalformedURLException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StsArnRole {
    private final String accountId;

    private StsArnRole(final String arn) {
        Pattern pattern = Pattern.compile("arn:aws:iam::(\\d{12}):role/.*");
        Matcher matcher = pattern.matcher(arn);
        String accountIdMatcher = null;
        if (matcher.matches()) {
            accountIdMatcher = matcher.group(1);
        }else{
            throw new IllegalArgumentException("ARN has accountId of invalid length.");
        }
        this.accountId = accountIdMatcher;
        if(Objects.nonNull(accountId) && !accountId.chars().allMatch(Character::isDigit)) {
            throw new IllegalArgumentException("ARN has accountId with invalid characters.");
        }
    }

    public String getAccountId() {
        return accountId;
    }

    public static StsArnRole parse(final String queueUrl) throws MalformedURLException {
        Objects.requireNonNull(queueUrl);
        return new StsArnRole(queueUrl);
    }
}
