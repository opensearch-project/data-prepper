package org.opensearch.dataprepper.plugins.source;

import software.amazon.awssdk.arns.Arn;

import java.util.Objects;
import java.util.Optional;

public class StsArnRole {
    private final String accountId;

    private StsArnRole(final String roleArn) {
        final Arn arnObj = Arn.fromString(roleArn);
        final Optional<String> accountId = arnObj.accountId();
        if (accountId.isPresent() && accountId.get().length() != 12)
            throw new IllegalArgumentException("ARN has accountId of invalid length.");
        if(!accountId.get().chars().allMatch(Character::isDigit))
            throw new IllegalArgumentException("ARN has accountId with invalid characters.");
        this.accountId = accountId.get();
    }

    public String getAccountId() {
        return accountId;
    }

    public static StsArnRole parse(final String roleArn) {
        Objects.requireNonNull(roleArn);
        return new StsArnRole(roleArn);
    }
}
