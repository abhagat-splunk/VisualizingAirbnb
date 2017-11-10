import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class CodeProfilerReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                        String resultText = "";
                        if(key.toString().equals("name")){
                                int maxNameLength = 0;
                                String maxLengthString = "";
                                for(Text value: values){
                                        String val = value.toString();
                                        if(val.length()>maxNameLength){
                                                maxNameLength = val.length();
                                                maxLengthString = val;
                                        }
                                }
                                resultText = "Max length of a string in names: "+Integer.toString(maxNameLength)+" , Max Length String: "+maxLengthString;        
                                context.write(key, new Text(resultText));
                        }
                        if(key.toString().equals("host_name")){
                                int maxNameLength = 0;
                                String maxLengthString = "";
                                for(Text value: values){
                                        String val = value.toString();
                                        if(val.length()>maxNameLength){
                                                maxNameLength = val.length();
                                                maxLengthString = val;
                                        }
                                }
                                resultText = "Max length of a string in host_names: "+Integer.toString(maxNameLength)+" , Max Length String: "+maxLengthString;        
                                context.write(key, new Text(resultText));
                        }
                        if(key.toString().equals("neighbourhood")){
                                int maxNameLength = 0;
                                String maxLengthString = "";
                                for(Text value: values){
                                        String val = value.toString();
                                        if(val.length()>maxNameLength){
                                                maxNameLength = val.length();
                                                maxLengthString = val;
                                        }
                                }
                                resultText = "Max length of a string in neighbourhood: "+Integer.toString(maxNameLength)+" , Max Length String: "+maxLengthString;        
                                context.write(key, new Text(resultText));
                        }
                        if(key.toString().equals("room_type")){
                                int maxNameLength = 0;
                                String maxLengthString = "";
                                for(Text value: values){
                                        String val = value.toString();
                                        if(val.length()>maxNameLength){
                                                maxNameLength = val.length();
                                                maxLengthString = val;
                                        }
                                }
                                resultText = "Max length of a string in room_type: "+Integer.toString(maxNameLength)+" , Max Length String: "+maxLengthString;        
                                context.write(key, new Text(resultText));
                        }
                        if(key.toString().equals("price")){
                                int minPrice = 100;
                                int maxPrice = 0;
                                for(Text value: values){
                                        int val = Integer.parseInt(value.toString());
                                        if(val>maxPrice){
                                                maxPrice = val;
                                        }
                                        if(val<minPrice){
                                                minPrice = val;
                                        }
                                }
                                resultText = "Max price of an apt/room: "+Integer.toString(maxPrice)+" , Minimum Price of a room/apt: "+Integer.toString(minPrice);        
                                context.write(key, new Text(resultText));
                        }
                        if(key.toString().equals("number_of_reviews")){
                                int minReviews = 100;
                                int maxReviews = 0;
                                for(Text value: values){
                                        int val = Integer.parseInt(value.toString());
                                        if(val>maxReviews){
                                                maxReviews = val;
                                        }
                                        if(val<minReviews){
                                                minReviews = val;
                                        }
                                }
                                resultText = "Max number of reviews: "+Integer.toString(maxReviews)+" , Minimum Number of reviews: "+Integer.toString(minReviews);        
                                context.write(key, new Text(resultText));
                        }

        }

}
