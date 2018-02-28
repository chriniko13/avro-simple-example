package com.chriniko.avro.examples;

import com.chriniko.avro.domain.Employee;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;

public class SecondExample implements Example {

    @Override
    public void run() {

        // Note: create an object...
        Employee employee = Employee.newBuilder()
                .setId(1)
                .setEmailAddress("emp1@comp.com")
                .setFirstName("first1")
                .setMiddleName("mid1")
                .setLastName("last1")
                .setUsername("user1")
                .build();

        File employeeAvroFile = new File("emp-avro.avro");

        // Note: serialize data to disk...
        serializeData(employee, employeeAvroFile);

        // Note: deserialize avro file and print contents...
        deserializeData(employeeAvroFile);
    }

    private void serializeData(Employee employee, File avroFile) {

        try {
            DatumWriter<Employee> employeeDatumWriter = new SpecificDatumWriter<>(Employee.class);

            DataFileWriter<Employee> employeeDataFileWriter = new DataFileWriter<Employee>(employeeDatumWriter);

            employeeDataFileWriter.create(employee.getSchema(), avroFile);
            employeeDataFileWriter.append(employee);
            employeeDataFileWriter.close();

        } catch (Exception error) {
            System.err.println("error --> " + error);
        }

    }

    private void deserializeData(File employeeAvroFile) {
        try {

            DatumReader<Employee> bdPersonDatumReader = new SpecificDatumReader<>(Employee.class);
            DataFileReader<Employee> dataFileReader = new DataFileReader<>(employeeAvroFile, bdPersonDatumReader);

            Employee emp = null;
            while (dataFileReader.hasNext()) {
                emp = dataFileReader.next(emp);
                System.out.println(emp);
            }

        } catch (Exception error) {
            System.err.println("error --> " + error);
        }
    }
}
