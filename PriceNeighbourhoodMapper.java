import java.io.*;
import java.util.*;
import java.*;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PriceNeighbourhoodMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
                String line = value.toString();
                String inp = line.trim().replaceAll(" +"," ");
                //Contains commas in the Name!
                if(inp.contains("\"")){
                        StringBuilder sb = new StringBuilder();
                        String[] inpList = inp.trim().split("\"");      
                        inpList[1] = inpList[1].replace(","," ");
                        for(String s: inpList){
                                sb.append(s);
                        }
                        inp = sb.toString();
                }

                String[] inpListCSV = inp.trim().split(",");
                if(inpListCSV.length==16){
                                context.write(new Text(inpListCSV[4]), new IntWritable(Integer.parseInt(inpListCSV[9]))); //Borough, Price
                }       
        }        
}