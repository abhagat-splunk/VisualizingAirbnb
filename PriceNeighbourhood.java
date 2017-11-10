import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class PriceNeighbourhood {
        public static void main(String[] args) throws Exception {
                if (args.length != 2) {
                System.err.println("Usage: Price Neighbourhood <input path> <output path>");
                System.exit(-1);
        }
        

    Job job = new Job(); 
    job.setJarByClass(PriceNeighbourhood.class); 
    job.setJobName("Average Price Neighbourhood");
    FileInputFormat.addInputPath(job, new Path(args[0])); 
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(PriceNeighbourhoodMapper.class);
    job.setReducerClass(PriceNeighbourhoodReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }

}