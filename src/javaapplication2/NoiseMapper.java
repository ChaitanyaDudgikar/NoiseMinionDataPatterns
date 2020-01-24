/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package javaapplication2;

import java.io.IOException;
import java.util.Calendar;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author cdudg
 */
public class NoiseMapper extends Mapper<Object, Text, Text, Text>
{

    Text outputKey = new Text();
    Text outputValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        //super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
        String line = value.toString();
        String[] tokens = line.split(",");

        double longitude = Double.parseDouble(tokens[3]);
        double latitude = Double.parseDouble(tokens[4]);
        double qlong = (int) (longitude * 1000) / 1000.0;
        double qlat = (int) (latitude * 1000) / 1000.0;
        String noiselevel = tokens[5];

        long time = Long.parseLong(tokens[2]);
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(time);

        c.add(Calendar.HOUR_OF_DAY, 5);
        c.add(Calendar.MINUTE, 30);

        int day = c.get(Calendar.DAY_OF_MONTH);
        int hour = c.get(Calendar.HOUR_OF_DAY);
        int dow = c.get(Calendar.DAY_OF_WEEK);
        int date = c.get(Calendar.DATE);
        int month = c.get(Calendar.MONTH);
        int year = c.get(Calendar.YEAR);
//        outputKey.set("DATE_" + date + "MONTH_" + month + "YEAR_" + year + "_" + qlong + "_" + qlat);
//        outputValue.set(longitude + "," + latitude + "," + noiselevel);
//        context.write(outputKey, outputValue);

        outputKey.set("DATE_" + date + "_" + month + "_" + year + "_" + hour + "_" + qlong + "_" + qlat);
        outputValue.set(longitude + "," + latitude + "," + noiselevel);
        context.write(outputKey, outputValue);

        
        outputKey.set("PATTERN-01_"  + qlong + "_" + qlat);
        outputValue.set(longitude + "," + latitude + "," + noiselevel);
        context.write(outputKey, outputValue);

        outputKey.set("PATTERN-02_" + hour + "_" + qlong + "_" + qlat);
        outputValue.set(longitude + "," + latitude + "," + noiselevel);
        context.write(outputKey, outputValue);

        outputKey.set("PATTERN-03_" + date + "_" + qlong + "_" + qlat);
        outputValue.set(longitude + "," + latitude + "," + noiselevel);
        context.write(outputKey, outputValue);
        
        outputKey.set("PATTERN-04_" + dow + "_" + qlong + "_" + qlat);
        outputValue.set(longitude + "," + latitude + "," + noiselevel);
        context.write(outputKey, outputValue);
        
        outputKey.set("PATTERN-05_" + date + "_" + hour + "_" + qlong + "_" + qlat);
        outputValue.set(longitude + "," + latitude + "," + noiselevel);
        context.write(outputKey, outputValue);

        outputKey.set("PATTERN-06_" + dow + "_" + hour + "_" + qlong + "_" + qlat);
        outputValue.set(longitude + "," + latitude + "," + noiselevel);
        context.write(outputKey, outputValue);

        outputKey.set("PATTERN-07_" + (month+1) + "_" + qlong + "_" + qlat);
        outputValue.set(longitude + "," + latitude + "," + noiselevel);
        context.write(outputKey, outputValue);
        
        outputKey.set("PATTERN-08_" + (month+1) + "_" + hour + "_" + qlong + "_" + qlat);
        outputValue.set(longitude + "," + latitude + "," + noiselevel);
        context.write(outputKey, outputValue);

        outputKey.set("PATTERN-09_" + (month+1) + "_" + date + "_" + qlong + "_" + qlat);
        outputValue.set(longitude + "," + latitude + "," + noiselevel);
        context.write(outputKey, outputValue);

        outputKey.set("PATTERN-10_" + (month+1) + "_" + dow + "_" + qlong + "_" + qlat);
        outputValue.set(longitude + "," + latitude + "," + noiselevel);
        context.write(outputKey, outputValue);

        outputKey.set("PATTERN-11_" + (month+1) + "_" + date + "_" + hour + "_" + qlong + "_" + qlat);
        outputValue.set(longitude + "," + latitude + "," + noiselevel);
        context.write(outputKey, outputValue);

        outputKey.set("PATTERN-12_" + (month+1) + "_" + dow + "_" + hour + "_" + qlong + "_" + qlat);
        outputValue.set(longitude + "," + latitude + "," + noiselevel);
        context.write(outputKey, outputValue);

    }
}
