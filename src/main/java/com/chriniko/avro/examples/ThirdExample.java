package com.chriniko.avro.examples;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ThirdExample implements Example {

    @Override
    public void run() {

        try {
            // create a record using generic record...
            String schemaLoc = "src/main/avro/employee.avsc";
            File schemaFile = new File(schemaLoc);

            Schema schema = new Schema.Parser().parse(schemaFile);

            GenericData.Record record = new GenericData.Record(schema);
            record.put("first_name", "first1");
            record.put("last_name", "last1");
            record.put("middle_name", "mid1");
            record.put("id", 1L);
            record.put("username", "user1");
            record.put("email_address", "email1");

            System.out.println("record = " + record);

            File employeeAvroFile = new File("emp-avro-gen.avro");

            // serialize record...
            final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);

            try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {

                dataFileWriter.create(record.getSchema(), employeeAvroFile);
                dataFileWriter.append(record);

            }

            //deserialize record...
            final List<GenericRecord> genericRecords = new ArrayList<>();

            final DatumReader<GenericRecord> empReader = new GenericDatumReader<>();
            final DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(employeeAvroFile, empReader);

            while (dataFileReader.hasNext()) {
                genericRecords.add(dataFileReader.next(null));
            }

            genericRecords.forEach(System.out::println);

        } catch (IOException error) {
            System.out.println("error ---> " + error);
        }

    }
}
