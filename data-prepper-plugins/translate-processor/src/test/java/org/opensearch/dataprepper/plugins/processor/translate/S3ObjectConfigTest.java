package org.opensearch.dataprepper.plugins.processor.translate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

class S3ObjectConfigTest {
    private S3ObjectConfig s3ObjectConfig;

    @BeforeEach
    void setup(){
        s3ObjectConfig = new S3ObjectConfig();
    }

    @Test
    void test_get_bucket() throws NoSuchFieldException, IllegalAccessException {
        String testBucket = "test-bucket";
        setField(S3ObjectConfig.class, s3ObjectConfig, "bucket", testBucket);
        assertThat(s3ObjectConfig.getBucket(), is(testBucket));
    }

    @Test
    void test_get_region() throws NoSuchFieldException, IllegalAccessException {
        String testRegion = "test-region";
        setField(S3ObjectConfig.class, s3ObjectConfig, "region", testRegion);
        assertThat(s3ObjectConfig.getRegion(), is(testRegion));
    }

    @Test
    void test_get_sts_role_arn() throws NoSuchFieldException, IllegalAccessException {
        String testRole = "arn:aws:iam::999999999999:role/s3-test-role";
        setField(S3ObjectConfig.class, s3ObjectConfig, "stsRoleArn", testRole);
        assertThat(s3ObjectConfig.getStsRoleArn(), is(testRole));
    }
}