package testinggg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HadoopProject extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        //Get the arguments
        Configuration conf = getConf();
        String[] myArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        int S = Integer.parseInt(myArgs[3]);
        int R = Integer.parseInt(myArgs[4]);
        int r = Integer.parseInt(myArgs[5]);

        
        //pass the S R r arguments to the configuration so that mappers can see them
        FileSystem fs= FileSystem.get(conf);
        fs.delete(new Path(args[2]), true);
        conf.set("S", String.valueOf(S));
        conf.set("R", String.valueOf(R));
        conf.set("r", String.valueOf(r));
 
        //Set the classes and start the job for the JOIN
        Job job = new Job(conf);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getSimpleName());
        job.setNumReduceTasks(r);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(OptimalReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        conf.set("mapreduce.jobtracker.split.metainfo.maxsize","-1");
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(myArgs[2]));               
        job.waitForCompletion(true);
        
        //2nd run , set new mappers and reducers for the SUM and GROUP BY and start the new Job
        Configuration conf2=getConf();
        fs= FileSystem.get(conf2);
        fs.delete(new Path(myArgs[1]), true);
        job = new Job(conf2);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getSimpleName());
        job.setMapperClass(MyMapper2.class);
        job.setReducerClass(MyReducer2.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        conf.set("mapreduce.jobtracker.split.metainfo.maxsize","-1");
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(myArgs[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        
        long startTime = System.currentTimeMillis();
        
        int rc = ToolRunner.run(new HadoopProject(), args);
        
        long endTime = System.currentTimeMillis();
        System.out.println("Time elapsed is " + (endTime - startTime) + " milliseconds");
        System.exit(rc);
        

    }

}
