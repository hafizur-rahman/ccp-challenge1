package com.jdreamer.ccp;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.Serializable;

/**
 * Created by bibagimon on 1/24/15.
 */
public class Task1Solution extends Configured implements Tool, Serializable {

    @Override
    public int run(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];

        // Create an object to coordinate pipeline creation and execution.
        Pipeline pipeline = new MRPipeline(Task1Solution.class, getConf());
        PCollection<String> lines = pipeline.readTextFile(inputPath);

        PCollection<String> v = lines.parallelDo(new DoFn<String, String>() {
            @Override
            public void process(String s, Emitter<String> emitter) {
                System.out.println(s);
            }
        }, Writables.strings());

        pipeline.writeTextFile(v, outputPath);
        pipeline.run();
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new Task1Solution(), args);
    }
}
