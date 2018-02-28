package com.chriniko.avro;

import com.chriniko.avro.examples.Example;
import com.chriniko.avro.examples.FirstExample;
import com.chriniko.avro.examples.SecondExample;
import com.chriniko.avro.examples.ThirdExample;

public class Main {

    public static void main(String[] args) {

        Example[] examples = {
                new FirstExample(),
                new SecondExample(),
                new ThirdExample()
        };

        int exampleToRun = 3; // Note: play only with this.

        examples[exampleToRun - 1].run();

    }
}
