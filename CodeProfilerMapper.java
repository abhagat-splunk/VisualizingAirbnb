import java.io.*;
import java.util.*;
import java.*;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CodeProfilerMapper extends Mapper<LongWritable, Text, Text, Text>{
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
                                context.write(new Text("name"), new Text(inpListCSV[1])); //name, name-value
                                context.write(new Text("host_name"), new Text(inpListCSV[3])); //host_name, host_name-value
                                context.write(new Text("neighbourhood_group"), new Text(inpListCSV[4])); //neighbourhood_group, neighbourhood_group-value
                                context.write(new Text("neighbourhood"), new Text(inpListCSV[5])); //neighbourhood, neighbourhood-value
                                context.write(new Text("room_type"), new Text(inpListCSV[8])); //room_type, room_type-value
                                context.write(new Text("price"), new Text(inpListCSV[9])); //price, price-value
                                context.write(new Text("number_of_reviews"), new Text(inpListCSV[11])); //number_of_reviews, number_of_reviews-value
                                        
                }       
        }        
}
