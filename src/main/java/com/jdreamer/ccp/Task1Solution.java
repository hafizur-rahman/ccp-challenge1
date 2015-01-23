package com.jdreamer.ccp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import org.apache.commons.io.FileUtils;
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

import java.io.File;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by bibagimon on 1/24/15.
 */
public class Task1Solution extends Configured implements Tool, Serializable {

    @Override
    public int run(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];

        // Delete old output
        FileUtils.deleteDirectory(new File(outputPath));

        // Create an object to coordinate pipeline creation and execution.
        Pipeline pipeline = new MRPipeline(Task1Solution.class, getConf());
        PCollection<String> lines = pipeline.readTextFile(inputPath);

        PCollection<String> v = lines.parallelDo(new DoFn<String, String>() {
            @Override
            public void process(String s, Emitter<String> emitter) {
                ObjectMapper mapper = new ObjectMapper();

                try {
                    final Session session = mapper.readValue(s, Session.class);

                    final Set<String> movies = new HashSet<String>();
                    movies.addAll(session.getPlayed().keySet());
                    movies.addAll(session.getRated().keySet());
                    movies.addAll(session.getReviewed().keySet());

                    String o = String.format("%s,%010d,%010d\t%s,%s",
                            session.getUser(), session.getStart(), session.getEnd(), session.getKid(),
                            Joiner.on(",").join(movies));

                    emitter.emit(o);
                } catch (Exception e) {
                    e.printStackTrace();
                }
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
