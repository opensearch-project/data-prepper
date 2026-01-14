# ✅ Kafka Backward Compatibility Test - Ready to Run!

## 🚀 Quick Start

### **Single Command:**

```bash
cd /Users/tylgry/data-prepper
./gradlew :e2e-test:kafka-backward-compatibility:kafkaBackwardCompatibilityTest
```

---

## 📋 **What It Does:**

1. ✅ Starts **Kafka** using Testcontainers (no manual setup needed!)
2. ✅ Starts **OpenSearch** container
3. ✅ Pulls **Data Prepper 2.10.0** from Docker Hub
4. ✅ Starts released Data Prepper → sends 2 test records → writes to Kafka
5. ✅ Stops released Data Prepper
6. ✅ Starts **current build** Data Prepper → reads from Kafka → writes to OpenSearch
7. ✅ Verifies both records are in OpenSearch
8. ✅ Cleanup all containers

**Expected time: ~5 minutes**

---

## ✅ **What Changed from Original Build.gradle Error:**

### **Before (Failed):**
- ❌ Tried to use `confluentinc/cp-kafka:3.6.0` (doesn't exist)
- ❌ Manual Docker Kafka container management
- ❌ Complex networking setup

### **After (Fixed):**
- ✅ Uses **Testcontainers** `KafkaContainer` (automatic Kafka management)
- ✅ Valid image: `confluentinc/cp-kafka:7.5.0`
- ✅ Kafka dependencies from Maven: `org.apache.kafka:kafka-clients:3.9.1`
- ✅ Simpler networking with `host.docker.internal`

---

## 🔧 **Prerequisites:**

```bash
# 1. Docker running
docker ps

# 2. Build Data Prepper Docker image first
cd /Users/tylgry/data-prepper
./gradlew :release:docker:docker
```

---

## 🎯 **Test Different Versions:**

```bash
# Test with Data Prepper 2.9.0
./gradlew :e2e-test:kafka-backward-compatibility:kafkaBackwardCompatibilityTest \
  -PbackwardCompatVersion=2.9.0

# Test with Data Prepper 2.8.0
./gradlew :e2e-test:kafka-backward-compatibility:kafkaBackwardCompatibilityTest \
  -PbackwardCompatVersion=2.8.0
```

---

## 🐛 **Troubleshooting:**

### **If test fails, check:**

```bash
# 1. Docker logs for Kafka (managed by Testcontainers)
docker logs $(docker ps -q --filter "ancestor=confluentinc/cp-kafka:7.5.0")

# 2. Released Data Prepper logs
docker logs data-prepper-writer

# 3. Current Data Prepper logs
docker logs data-prepper-reader

# 4. OpenSearch logs
docker logs node-0.example.com
```

### **Clean up stuck containers:**

```bash
docker stop data-prepper-writer data-prepper-reader
docker rm data-prepper-writer data-prepper-reader
```

---

## 📊 **Key Changes Made:**

### **Files Created:**
```
e2e-test/kafka-backward-compatibility/
├── build.gradle (Testcontainers + Docker orchestration)
├── README.md (Full documentation)
└── src/integrationTest/
    ├── java/.../KafkaBackwardCompatibilityTest.java (Test logic with Testcontainers)
    └── resources/
        ├── writer-pipeline.yaml (HTTP → Kafka)
        ├── reader-pipeline.yaml (Kafka → OpenSearch)
        └── data-prepper-config.yaml
```

### **Key Dependencies Added:**
```gradle
'org.apache.kafka:kafka-clients:3.9.1'
'org.apache.kafka:connect-json:3.9.1'
'org.testcontainers:testcontainers:1.19.0'
'org.testcontainers:kafka:1.19.0'
```

---

## ✅ **Ready to Run!**

```bash
cd /Users/tylgry/data-prepper && \
./gradlew :e2e-test:kafka-backward-compatibility:kafkaBackwardCompatibilityTest
```

🎉 **That's it!** The test will automatically manage Kafka, verify backward compatibility, and clean up!
