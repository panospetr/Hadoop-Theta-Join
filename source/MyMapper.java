package testinggg;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;

/**
 * A class that extends Hadoop's Mapper class , used to sent Tuples to the assigned partition
 *
  */
public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //Initialize the variables of the partition
        Configuration conf = context.getConfiguration();
        String Sstr = conf.get("S");
        String Rstr = conf.get("R");
        String rstr = conf.get("r");
        
        //MediocrePartitioner instace that stores data for the Lookup table
        MediocrePartitioner myMatrix = new MediocrePartitioner(Integer.parseInt(Sstr), Integer.parseInt(Rstr), Integer.parseInt(rstr));

        //split the tuple-line and assign it to partitioned rejions        
        String[] split = value.toString().split(",");
        int myint = Integer.parseInt(split[1]); //The R.a value

        //R.a<S.a , R.a>10 so it must be S.a>10 too
        if ((myint > 10)) {
            //if its R tuple get column else get row
            if (split[0].equals("R")) {
                ArrayList myList = myMatrix.getRegions("R");
                for (int i = 0; i < myList.size(); i++) {
                    Text myText = new Text();
                    String str = String.valueOf(myList.get(i));
                    myText.set(str);
                    context.write(myText, value);  //emit the <assigned-partition, all-the-tuple>
                }

            } else if (split[0].equals("S")) {
                ArrayList myList = myMatrix.getRegions("S");
                for (int i = 0; i < myList.size(); i++) {
                    Text myText = new Text();
                    String str = String.valueOf(myList.get(i));
                    myText.set(str);
                    context.write(myText, value); //emit the <assigned-partition, all-the-tuple>
                }
            }
        }

    }
}
