package com.aws.aurora.cdc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AuroraCDCToS3Tables {
    
    public static void main(String[] args) throws Exception {
        // Get configuration from environment or args
        String kafkaBootstrapServers = getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", 
            "b-2.auroracdccluster.5yymx6.c18.kafka.us-east-1.amazonaws.com:9092,b-1.auroracdccluster.5yymx6.c18.kafka.us-east-1.amazonaws.com:9092");
        String kafkaTopic = getEnvOrDefault("KAFKA_TOPIC", "aurora.cdc.public.customers");
        String s3TablesBucketArn = getEnvOrDefault("S3_TABLES_BUCKET_ARN", "");
        String namespace = getEnvOrDefault("S3_TABLES_NAMESPACE", "aurora_cdc");
        String tableName = getEnvOrDefault("S3_TABLES_TABLE", "customers");
        
        // Set up Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000); // Checkpoint every 60 seconds
        
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        
        // Configure Kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaBootstrapServers)
            .setTopics(kafkaTopic)
            .setGroupId("flink-cdc-consumer")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
        
        // Read from Kafka
        DataStream<String> kafkaStream = env
            .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        
        // Register Iceberg catalog for S3 Tables
        tableEnv.executeSql(String.format(
            "CREATE CATALOG s3tables WITH (" +
            "  'type' = 'iceberg'," +
            "  'catalog-impl' = 'software.amazon.s3tables.iceberg.S3TablesCatalog'," +
            "  'warehouse' = '%s'" +
            ")", s3TablesBucketArn));
        
        tableEnv.executeSql("USE CATALOG s3tables");
        
        // Create namespace if not exists
        tableEnv.executeSql(String.format(
            "CREATE NAMESPACE IF NOT EXISTS %s", namespace));
        
        // Create table if not exists (example schema for customers)
        tableEnv.executeSql(String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s (" +
            "  customer_id INT," +
            "  first_name STRING," +
            "  last_name STRING," +
            "  email STRING," +
            "  phone STRING," +
            "  registration_date STRING," +
            "  customer_tier STRING," +
            "  created_at TIMESTAMP(3)," +
            "  updated_at TIMESTAMP(3)," +
            "  PRIMARY KEY (customer_id) NOT ENFORCED" +
            ") WITH (" +
            "  'format-version' = '2'," +
            "  'write.upsert.enabled' = 'true'" +
            ")", namespace, tableName));
        
        // Convert Kafka stream to Table
        tableEnv.createTemporaryView("kafka_source", kafkaStream);
        
        // Parse Debezium CDC events and extract 'after' payload
        Table cdcTable = tableEnv.sqlQuery(
            "SELECT " +
            "  CAST(JSON_VALUE(value, '$.after.customer_id') AS INT) as customer_id," +
            "  JSON_VALUE(value, '$.after.first_name') as first_name," +
            "  JSON_VALUE(value, '$.after.last_name') as last_name," +
            "  JSON_VALUE(value, '$.after.email') as email," +
            "  JSON_VALUE(value, '$.after.phone') as phone," +
            "  JSON_VALUE(value, '$.after.registration_date') as registration_date," +
            "  JSON_VALUE(value, '$.after.customer_tier') as customer_tier," +
            "  TO_TIMESTAMP(FROM_UNIXTIME(CAST(JSON_VALUE(value, '$.after.created_at') AS BIGINT) / 1000)) as created_at," +
            "  TO_TIMESTAMP(FROM_UNIXTIME(CAST(JSON_VALUE(value, '$.after.updated_at') AS BIGINT) / 1000)) as updated_at " +
            "FROM kafka_source " +
            "WHERE JSON_VALUE(value, '$.after') IS NOT NULL"
        );
        
        // Insert into S3 Tables
        cdcTable.executeInsert(String.format("%s.%s", namespace, tableName));
        
        env.execute("Aurora CDC to S3 Tables");
    }
    
    private static String getEnvOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }
}
