package com.rinalds;
import com.hortonworks.registries.schemaregistry.*;
import org.apache.avro.Schema;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class SchemaRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistry.class);
    public static final String DEFAULT_SCHEMA_REG_URL = "http://35.197.234.216/api/v1";
    private static Map<String, Object> config = createConfig(DEFAULT_SCHEMA_REG_URL);

    public SchemaRegistry(Map<String, Object> config){
        this.config = config;
    }

    // Reference to examples
    public static void runExamples(){
//        SchemaMetadata schemaMetadata = createSchemaMetadata("com.rinalds.sample-" + System.currentTimeMillis());
//        System.out.println(schemaMetadata.toString());
//        String schemaFileName = "/device.avsc";
//        String schema1 = getSchema(schemaFileName);
//        System.out.println(schema1);
//
//        SchemaRegistryClient schemaRegistryClient = new SchemaRegistryClient(config);
//        // registering a new schema
//        SchemaVersion schemaVersion1 = new SchemaVersion(schema1, "Initial version of the schema");
//        SchemaIdVersion v1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, schemaVersion1);
//        LOG.info("Registered schema metadata [{}] and returned version [{}]", schema1, v1);
//
//        // adding a new version of the schema
//        String schema2 = getSchema("/device-new.avsc");
//        SchemaVersion schemaInfo2 = new SchemaVersion(schema2, "second version");
//        SchemaIdVersion v2 = schemaRegistryClient.addSchemaVersion(schemaMetadata, schemaInfo2);
//        LOG.info("Registered schema metadata [{}] and returned version [{}]", schema2, v2);
//
//        //adding same schema returns the earlier registered version
//        SchemaIdVersion version = schemaRegistryClient.addSchemaVersion(schemaMetadata, schemaInfo2);
//        LOG.info("Received version [{}] for schema metadata [{}]", version, schemaMetadata);
//
//        // get a specific version of the schema
//        String schemaName = schemaMetadata.getName();
//        SchemaVersionInfo schemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(new SchemaVersionKey(schemaName, v2.getVersion()));
//        LOG.info("Received schema version info [{}] for schema metadata [{}]", schemaVersionInfo, schemaMetadata);
//        System.out.println("Received schema version info [{}] for schema metadata [{}]"schemaVersionInfo, schemaMetadata);
//
//        // get latest version of the schema
//        SchemaVersionInfo latest = schemaRegistryClient.getLatestSchemaVersionInfo(schemaName);
//        LOG.info("Latest schema with schema key [{}] is : [{}]", schemaMetadata, latest);
//
//        // get all versions of the schema
//        Collection<SchemaVersionInfo> allVersions = schemaRegistryClient.getAllVersions(schemaName);
//        LOG.info("All versions of schema key [{}] is : [{}]", schemaMetadata, allVersions);
//
//        // finding schemas containing a specific field
//        SchemaFieldQuery md5FieldQuery = new SchemaFieldQuery.Builder().name("md5").build();
//        Collection<SchemaVersionKey> md5SchemaVersionKeys = schemaRegistryClient.findSchemasByFields(md5FieldQuery);
//        LOG.info("Schemas containing field query [{}] : [{}]", md5FieldQuery, md5SchemaVersionKeys);
//
//        SchemaFieldQuery txidFieldQuery = new SchemaFieldQuery.Builder().name("txid").build();
//        Collection<SchemaVersionKey> txidSchemaVersionKeys = schemaRegistryClient.findSchemasByFields(txidFieldQuery);
//        LOG.info("Schemas containing field query [{}] : [{}]", txidFieldQuery, txidSchemaVersionKeys);
    }

    public static boolean runAvroSerDesApis() throws IOException {
        //using builtin avro serializer/deserializer
        AvroSnapshotSerializer avroSnapshotSerializer = new AvroSnapshotSerializer();
        avroSnapshotSerializer.init(config);
        AvroSnapshotDeserializer avroSnapshotDeserializer = new AvroSnapshotDeserializer();
        avroSnapshotDeserializer.init(config);

        Object deviceObject = createGenericRecordForDevice("/device-latest.avsc");

        SchemaMetadata schemaMetadata = createSchemaMetadata("avro-serializer-schema-" + System.currentTimeMillis());
        byte[] serializedData = avroSnapshotSerializer.serialize(deviceObject, schemaMetadata);
        Object deserializedObj = avroSnapshotDeserializer.deserialize(new ByteArrayInputStream(serializedData), null);

        LOG.info("Serialized and deserialized objects are equal: [{}] ", deviceObject.equals(deserializedObj));
        System.out.println("Deserialized: " + deserializedObj.toString());
        System.out.println("DeviceObject: " + deviceObject.toString());
        return deserializedObj.toString().equals(deviceObject.toString());
    }

    protected static Object createGenericRecordForDevice(String schemaFileName) throws IOException {
        Schema schema = new Schema.Parser().parse(getSchema(schemaFileName));
        System.out.println(schema);
        GenericRecord avroRecord = new GenericData.Record(schema);
        long now = System.currentTimeMillis();
        avroRecord.put("xid", now);
        avroRecord.put("name", "foo-" + now);
        avroRecord.put("version", new Random().nextInt());
        avroRecord.put("timestamp", now);
        avroRecord.put("make", "Company");
        avroRecord.put("model", "X1");

        return avroRecord;
    }

    private static Map<String, Object> createConfig(String schemaRegistryUrl) {
        Map<String, Object> config = new HashMap();
        config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), schemaRegistryUrl);
        config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_SIZE.name(), 10L);
        config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS.name(), 5000L);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_SIZE.name(), 1000L);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS.name(), 60 * 60 * 1000L);
        return config;
    }

    private static SchemaMetadata createSchemaMetadata(String name) {
        return new SchemaMetadata.Builder(name)
                .type(AvroSchemaProvider.TYPE)
                .schemaGroup("test")
                .description("Sample schema")
                .compatibility(SchemaCompatibility.BOTH)
                .build();
    }

    private static String getSchema(String schemaFileName) throws IOException {
        InputStream schemaResourceStream = SchemaRegistry.class.getResourceAsStream(schemaFileName);
        if (schemaResourceStream == null) {
            throw new IllegalArgumentException("Given schema file [" + schemaFileName + "] does not exist");
        }
        return IOUtils.toString(schemaResourceStream, "UTF-8");
    }

    public static void main(String... args) throws IOException, SchemaNotFoundException, InvalidSchemaException, IncompatibleSchemaException {
        SchemaRegistry schemaRegistry = new SchemaRegistry(createConfig(DEFAULT_SCHEMA_REG_URL));

        SchemaMetadata schemaMetadata = createSchemaMetadata("com.testcompany.sample");
        String schemaFileName = "/device-latest.avsc";
        String deviceSchema = getSchema(schemaFileName);

        SchemaRegistryClient schemaRegistryClient = new SchemaRegistryClient(config);

        // adding new latest schema
        SchemaVersion schemaInfo2 = new SchemaVersion(deviceSchema, "third version");
        SchemaIdVersion latestToAdd = schemaRegistryClient.addSchemaVersion(schemaMetadata, schemaInfo2); // update to newest version

        // get the name of schema based on metadata
        String schemaName = schemaMetadata.getName();

        // fetching for latest from server
        SchemaVersionInfo latestServer = schemaRegistryClient.getLatestSchemaVersionInfo(schemaName);
        System.out.println("Latest version on server: " + latestServer.getVersion().toString());

        SchemaVersionInfo serverSchemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(new SchemaVersionKey(schemaName, latestServer.getVersion()));
        System.out.println(serverSchemaVersionInfo.getVersion() == 3);

        System.out.println("Serialized/Deserialized returns the same: " + runAvroSerDesApis());
    }
}
