/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package javaapplication2;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author cdudg
 */
public class JavaApplication2
{

//    private static class NoiseInputFormat extends InputFormat
//    {
//
//        public NoiseInputFormat()
//        {
//        }
//
//        @Override
//        public List getSplits(JobContext jc) throws IOException, InterruptedException
//        {
//            return new ArrayList<>();
//        }
//
//        @Override
//        public RecordReader createRecordReader(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException
//        {
//            return new RecordReader()
//            {
//                int val=0;
//                @Override
//                public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException
//                {
//                }
//
//                @Override
//                public boolean nextKeyValue() throws IOException, InterruptedException
//                {
//                    return (val<100);
//                }
//
//                @Override
//                public Object getCurrentKey() throws IOException, InterruptedException
//                {
//                    return ++val;
//                }
//
//                @Override
//                public Object getCurrentValue() throws IOException, InterruptedException
//                {
//                    return val*val;
//                }
//
//                @Override
//                public float getProgress() throws IOException, InterruptedException
//                {
//                    return val/100f;
//                }
//
//                @Override
//                public void close() throws IOException
//                {
//                }
//            };
//        }
//    }
//
//    private static class NouseOutputFormat extends OutputFormat
//    {
//
//        public NouseOutputFormat()
//        {
//        }
//
//        @Override
//        public RecordWriter getRecordWriter(TaskAttemptContext tac) throws IOException, InterruptedException
//        {
//            return new RecordWriter()
//            {
//                @Override
//                public void write(Object k, Object v) throws IOException, InterruptedException
//                {
//                    System.out.println("Record Writer : "+k+" : "+v);
//                }
//
//                @Override
//                public void close(TaskAttemptContext tac) throws IOException, InterruptedException
//                {
//                }
//            };
//        }
//
//        @Override
//        public void checkOutputSpecs(JobContext jc) throws IOException, InterruptedException
//        {
//        }
//
//        @Override
//        public OutputCommitter getOutputCommitter(TaskAttemptContext tac) throws IOException, InterruptedException
//        {
//            return new OutputCommitter()
//            {
//                @Override
//                public void setupJob(JobContext jc) throws IOException
//                {
//                }
//
//                @Override
//                public void setupTask(TaskAttemptContext tac) throws IOException
//                {
//                }
//
//                @Override
//                public boolean needsTaskCommit(TaskAttemptContext tac) throws IOException
//                {
//                    return false;
//                }
//
//                @Override
//                public void commitTask(TaskAttemptContext tac) throws IOException
//                {
//                }
//
//                @Override
//                public void abortTask(TaskAttemptContext tac) throws IOException
//                {
//                }
//            };
//        }
//    }
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, Exception, Exception
    {
 //       System.setProperty("hadoop.home.dir", "d:/hadoop-3.0.0");

//        Configuration conf = new Configuration();
        
        File outputDir=new File("output");
        if(outputDir.exists())
        {
            //Files.(outputDir.toPath());
            File f=new File("output_"+System.currentTimeMillis());
            outputDir.renameTo(f);
        }
        
        InputDataGenerator.generateInputData();
        
        Job job = Job.getInstance();
        job.setJarByClass(JavaApplication2.class);

        job.setMapperClass(NoiseMapper.class);
        job.setReducerClass(NoiseReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
       
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        

//        job.setInputFormatClass(NoiseInputFormat.class);
//        job.setOutputFormatClass(NouseOutputFormat.class);
        job.setNumReduceTasks(1);

//        FileInputFormat.addInputPath(job, new Path("input"));
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        job.waitForCompletion(true);
    }

}
