/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package javaapplication2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author cdudg
 */
public class NoiseReducer extends Reducer<Text, Text, Text, Text>
{

    Text outputKey = new Text();
    Text outputValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        outputKey.set(key);
        int n = 0;
        double total = 0;
        double ntotal = 0;
        double max=0;
        double min=500;
        for (Text value : values)
        {
            String line = value.toString();
            String[] tokens = line.split(",");

            double noiselevel = Double.parseDouble(tokens[2]);
            double nnoiselevel = Math.pow(10, noiselevel / 20);
            total += noiselevel;
            ntotal += nnoiselevel;
            if (noiselevel>max)
            {
                max=noiselevel;
            }
            if(noiselevel<min)
            {
                min=noiselevel;
            }
            n++;
        }
        double avg = (total / n);
        double navg = 20 * Math.log10(ntotal / n);
        outputValue.set(avg + " normalize_" + navg+" n_"+n+" max_"+max+" min_"+min);
        context.write(outputKey, outputValue);
    }
}
