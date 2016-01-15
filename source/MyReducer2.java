package testinggg;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer2 extends Reducer<IntWritable, Text, IntWritable, Text> {

    /**
     * The reducer function recieves all the X values for a specific R.a value
     * and produces the SUM(X). It emits a <R.a, SUM(x)> pair.
     * GROUP by is done on its own via the hadoop . We achieve numerical sorting on the output file by having IntWritable keys.
     *
     * @param key the R.a value
     * @param values all the X values that had the same R.a in the input
     * @param output
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(IntWritable key, Iterable<Text> values, Context output)
            throws IOException, InterruptedException {

        Iterator itr = values.iterator();
        long myValue = 0;
        // Compute the sum and emit the <R.a, SUM> pair;
        while (itr.hasNext()) {
            String myValueStr = itr.next().toString();
            myValue += Long.parseLong(myValueStr);
        }
        Text myText = new Text();
        myText.set(String.valueOf(myValue));
        String strKey = key.toString();
        IntWritable intKey = new IntWritable();
        intKey.set(Integer.parseInt(strKey));
        output.write(intKey, myText);

    }
}
