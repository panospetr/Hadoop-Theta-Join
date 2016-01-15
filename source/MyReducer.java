/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package testinggg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Text, Text, Text>{
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context output)
            throws IOException, InterruptedException {
        
        ArrayList mylist1 = new ArrayList<String>();
        ArrayList mylist2 = new ArrayList<String>();
        
        Iterator itr=values.iterator();
        
        while(itr.hasNext())
        {
            String tuple=itr.next().toString();
            String[] split = tuple.split(",");
            if(split[0].equals("R")){
                mylist1.add(tuple);
            }
            else 
            {
                mylist2.add(tuple);
            }
        }
        for(int i=0;i<mylist1.size();i++){
            String tupleR=(String)mylist1.get(i);
            String[] splitR=tupleR.split(",");
            int valueR=Integer.parseInt(splitR[1]);
            boolean flag=false;
            int topicalSum=0;
            for(int j=0;j<mylist2.size();j++){
                
                String tupleS=(String)mylist2.get(j);
                String[] splitS=tupleS.split(",");
                int valueS=Integer.parseInt(splitS[1]);
                if(valueR < valueS)
                {
                    int valueX=Integer.parseInt(splitS[2]);
                    topicalSum+=valueX;
                    flag=true;
                }
            }
            
                if(flag)
                {
                    Text tupleCombo =new Text();
                    flag=false;
                    String strTemp=splitR[1]+" "+topicalSum;
                    tupleCombo.set(strTemp);
                    output.write(null,tupleCombo);
                }
        }
        
    }
}
