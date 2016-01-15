
package testinggg;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**An optimalReducer extends Hadoop's Reducer class (makes up for a MediocrePartitioner). 
 * Computes the join for each R.a < S.a and produces a <R.a TopicSum> pair where topicSum is the sum of all S.x values 
 * joined to a R.a value in this partition
 * 
 *
 */
public class OptimalReducer extends Reducer<Text, Text, Text, Text> {

    /**Reduce function implements a join algorithm for the list of tuples assigned to it. Emits a <null, R.a SUM(x)> pair where
     * SUM(S.x) is the topical sum of all the S.x value of tuples with S.a > R.a in the current partition
     * At first R tuples are put into an ArrayList and S tuples are put into a TreeMap with S.a as key and value a List of 
     * all the tuples with the same key. Next, every R tuple is compared to each S.a value of the TreeMap's keySet (which is in
     * descending order). While R,a < S.a the programm produces a join with the contained list for each S.a . Otherwise 
     * it breaks the loop.(keys are sorted in descending order)
     * @param key       the partitioner number , not used
     * @param values    a list of the tuples assigned to this reducer
     * @param output    
     * @throws IOException
     * @throws InterruptedException 
     */
    public void reduce(Text key, Iterable<Text> values, Context output)
            throws IOException, InterruptedException {

        ArrayList mylist1 = new ArrayList<String>(); // list that will contain R tuples
        TreeMap<Integer, ArrayList<String>> myMap = new TreeMap<Integer, ArrayList<String>>(); //TreeMap that will contain the S tuples

        Iterator itr = values.iterator();
 
        // Loop that iterates through the given tuples and assigns them to the ArrayList or the TreeMap 
        while (itr.hasNext()) {
            String tuple = itr.next().toString();
            String[] split = tuple.split(",");
            if (split[0].equals("R")) {
                mylist1.add(tuple);
            } else {
                int avalue=Integer.parseInt(split[1]);
                ArrayList tempList;
                if(myMap.containsKey(avalue))
                {
                   tempList=(ArrayList)myMap.get(avalue);
                   tempList.add(tuple);
                   myMap.put(avalue, tempList);
                }
                else
                {
                    tempList=new ArrayList<String>();
                    tempList.add(tuple);
                    myMap.put(avalue, tempList);
                }
            }
        }
        
        //The join algorithm 
        /*
        Every R tuple is compared to each S.a value of the TreeMap's keySet (which is in
        descending order). While R,a < S.a the programm produces a join with the contained list for each S.a . Otherwise 
        it breaks the loop.(keys are sorted in descending order)
        */
        
        for (int i = 0; i < mylist1.size(); i++) {
            String tupleR = (String) mylist1.get(i);
            String[] splitR = tupleR.split(",");
            int valueR = Integer.parseInt(splitR[1]);
            boolean flag = false;
            long topicalSum = 0;
            Set keySet = myMap.descendingKeySet();
            Iterator myIter = keySet.iterator();
            while (myIter.hasNext()) {

                int currentAofS = (Integer) myIter.next();
                if (valueR < currentAofS) {
                    ArrayList<String> tempList=myMap.get(currentAofS);
                    for(String tupleS: tempList)
                    {
                        String[] splitS = tupleS.split(",");
                        long valueX = Long.parseLong(splitS[2]);
                        topicalSum += valueX;   // reduce the joined tuples produced for each R.a value by emiting a <R.a,regional-Sum> pair
                        flag = true;
                    }
                } 
                else {
                    break;
                }
            }
            
            if (flag) {
                Text tupleCombo = new Text();
                flag = false;
                String strTemp = splitR[1] + " " + topicalSum;
                tupleCombo.set(strTemp);
                output.write(null, tupleCombo);
            }
        }

    }
}
