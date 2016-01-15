
package testinggg;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;

/**MyMapper2 , used to sent all lines with the same R.a value to the same reducer.
 * 
 * 
 */
public class MyMapper2 extends Mapper<LongWritable, Text, IntWritable, Text> {

    /** The map function recieves a "R.a X" value and emits a <R.a , X> pair 
     * @param key
     * @param value a "R.a X" line
     * @param context
     * @throws IOException
     * @throws InterruptedException 
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        //Split the given line and emit a <Split[0] , Split[1]> pair
        String[] split = value.toString().split(" ");
        String myKey=split[0];
        System.out.println(myKey+" "+value.toString());
        String myValue=split[1];
        
        IntWritable intKey = new IntWritable();
        intKey.set(Integer.parseInt(myKey));
        
        Text myText2=new Text();
        
        myText2.set(myValue);
        context.write(intKey, myText2);
        
    }
}