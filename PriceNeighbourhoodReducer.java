import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class PriceNeighbourhoodReducer extends Reducer<Text, IntWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                        double sumOfValues = 0.00;
                        int counter = 0;
                        int highestPrice = 0;
                        int lowestPrice = 100;
                        for(IntWritable value: values){
                                int tempPrice = value.get();
                                if(tempPrice>highestPrice){
                                        highestPrice=tempPrice;
                                }
                                if(tempPrice<lowestPrice && tempPrice>0){
                                        lowestPrice=tempPrice;
                                }
                                sumOfValues += (double) tempPrice;
                                counter+=1;
                        }
                        double resultValue = sumOfValues/counter;
                        String resultText = "Average Price in this neighbourhood: " + Double.toString(resultValue) + ", Highest Price in this neighbourhood: "+ Integer.toString(highestPrice)+ ", Lowest Price in this neighbourhood: "+Integer.toString(lowestPrice);
                        context.write(key, new Text(resultText));
        }
}