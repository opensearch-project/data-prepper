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
| 2         | 1               | 5546       |
| 2         | 2               | 6562       |
| 2         | 4               | 6060       |
| 2         | 8               | 5330       |
| 2         | 16              | 4877       |
| 4         | 1               | 6149       |
| 4         | 2               | 9372       |
| 4         | 4               | 10482       |
| 4         | 8               | 9924       |
| 4         | 16              | 8513       |
| 8         | 1               | 5838       |
| 8         | 2               | 10803       |
| 8         | 4               | 18114       |
| 8         | 8               | 15708       |
| 8         | 16              | 16719       |
| 16        | 1               | 5986       |
| 16        | 2               | 10773       |
| 16        | 4               | 20157       |
| 16        | 8               | 21387      |
| 16        | 16              | 34537      |