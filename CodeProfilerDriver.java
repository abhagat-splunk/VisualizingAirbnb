import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class CodeProfilerDriver {
        public static void main(String[] args) throws Exception {
                if (args.length != 2) {
                System.err.println("Usage: Code Profiler <input path> <output path>");
                System.exit(-1);
        }
        

    Job job = new Job(); 
    job.setJarByClass(CodeProfilerDriver.class); 
    job.setJobName("Code Profiler Driver");
    FileInputFormat.addInputPath(job, new Path(args[0])); 
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(CodeProfilerMapper.class);
    job.setReducerClass(CodeProfilerReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }

}
