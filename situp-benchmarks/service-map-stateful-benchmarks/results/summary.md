# Benchmarking Results

## Setup
These tests were run on AWS EC2 r5 generation instances. For benchmarking with CPU counts of 2, 4, 8, and 16, 
instance types of r5.large, r5.xlarge, r5.2xlarge, and r5.4xlarge were used respectively. 

For the benchmarking, iterations were run for 10 minutes, with the default window duration of 3 minutes. The batch size
used, in terms of Records, was 1024. Throughput is represented in terms of Records processed per second, rather than
batches processed per second.

## Results

| CPU Count | Processor Count | Throughput |
|-----------|-----------------|------------|
| 2         | 1               | 4965       |
| 2         | 2               | 5948       |
| 2         | 4               | 5270       |
| 2         | 8               | 4863       |
| 2         | 16              | 4566       |
| 4         | 1               | 5095       |
| 4         | 2               | 7831       |
| 4         | 4               | 8011       |
| 4         | 8               | 8022       |
| 4         | 16              | 7230       |
| 8         | 1               | 5578       |
| 8         | 2               | 6770       |
| 8         | 4               | 8562       |
| 8         | 8               | 9821       |
| 8         | 16              | 9810       |
| 16        | 1               | 5696       |
| 16        | 2               | 7391       |
| 16        | 4               | 9584       |
| 16        | 8               | 13554      |
| 16        | 16              | 12151      |