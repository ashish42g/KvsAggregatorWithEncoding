package com.apple.irpt.df.jobs.kvs.dataWrite;

import com.apple.irpt.df.jobs.kvs.dataWrite.mapreduce.KVSCombiner;
import com.apple.irpt.df.jobs.kvs.dataWrite.mapreduce.KVSMapper1;
import com.apple.irpt.df.jobs.kvs.dataWrite.mapreduce.KVSMapper2;
import com.apple.irpt.df.jobs.kvs.dataWrite.mapreduce.KVSReducer;
import com.apple.irpt.df.jobs.kvs.dataWrite.schema.aggrDaily.Avro_aggrDaily_KVSDlyAggr0;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static com.apple.irpt.df.jobs.kvs.dataWrite.utility.KVSUtils.*;

public class Driver extends Configured implements Tool {

    private static final Logger LOGGER = Logger.getLogger(Driver.class);

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Driver(), args));
    }

    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

        if (args.length < 3) {
            LOGGER.error("This job expects five command line arguments : \n " +
                    "args[0] is input file path, \n" +
                    "args[1] is mapping file path, \n" +
                    "args[2] Output (Full and partial encoded data) file path, \n" +
                    "args[3] is new mapping file and \n");
            return 1;
        }

        LOGGER.info("Job Input : " + args[0]);
        LOGGER.info("Mapping File : " + args[1]);
        LOGGER.info("Intermediate Output directory : " + args[2]);
        LOGGER.info("New Mapped File : " + args[3]);

        Configuration config = getConf();

        Job job1 = new Job(config, getClass().getName());
        DistributedCache.addCacheFile(new URI(args[1]), job1.getConfiguration());

        job1.getConfiguration().set(UNMAPPED_KEY, args[2] + UNMAPPED_KEY + "/");
        job1.getConfiguration().set(PARTIAL_ENCODED_DATA, args[2] + PARTIAL_ENCODED_DATA + "/");
        job1.getConfiguration().set(INITIAL_ENCODED_DATA, args[2] + INITIAL_ENCODED_DATA + "/");

        MultipleOutputs.addNamedOutput(job1, UNMAPPED_KEY, TextOutputFormat.class, Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job1, PARTIAL_ENCODED_DATA, TextOutputFormat.class, Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job1, INITIAL_ENCODED_DATA, TextOutputFormat.class, Text.class, NullWritable.class);

        FileInputFormat.addInputPaths(job1, args[0]);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));

        job1.setInputFormatClass(AvroKeyInputFormat.class);
        AvroJob.setInputKeySchema(job1, Avro_aggrDaily_KVSDlyAggr0.SCHEMA$);

        job1.setJarByClass(Driver.class);

        job1.setMapperClass(KVSMapper1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(NullWritable.class);

        job1.setCombinerClass(KVSCombiner.class);
        job1.setReducerClass(KVSReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);

        job1.setNumReduceTasks(1);

        if (job1.waitForCompletion(true)) {
            Job job2 = new Job(config, getClass().getName());
            DistributedCache.addCacheFile(new URI(args[2] + UNMAPPED_KEY + "/"), job2.getConfiguration());
            job2.getConfiguration().set(FULL_ENCODED_DATA, args[2] + FULL_ENCODED_DATA + "/");

            FileInputFormat.addInputPaths(job2, args[2] + "/" + PARTIAL_ENCODED_DATA + "/*");
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));
            MultipleOutputs.addNamedOutput(job2, FULL_ENCODED_DATA, TextOutputFormat.class, Text.class, NullWritable.class);

            job2.setInputFormatClass(TextInputFormat.class);

            job2.setJarByClass(Driver.class);
            job2.setMapperClass(KVSMapper2.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(NullWritable.class);

            job2.setNumReduceTasks(0);

            job2.waitForCompletion(true);
        }
        return 0;
    }
}
