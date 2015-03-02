package com.edentech.foodpyramid;




import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class XmlDriver {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            runJob(args[0], args[1]);

        } catch (IOException ex) {
            Logger.getLogger(XmlDriver.class.getName()).log(Level.SEVERE, null, ex);
        }
    }


    public static void runJob(String input,
                              String output ) throws IOException {

        Configuration conf = new Configuration();

        // set your XML tags here
        conf.set("xmlinput.start", "<Food_Display_Row>");
        conf.set("xmlinput.end", "</Food_Display_Row>");
        conf.set("io.serializations",
                "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

        Job job = new Job(conf, "xml parser");


        FileInputFormat.setInputPaths(job, input);
        job.setJarByClass(XmlDriver.class);
        job.setMapperClass(XmlMapper.class);
        job.setInputFormatClass(XmlInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(XmlReducer.class);

        Path outPath = new Path(output);
        FileOutputFormat.setOutputPath(job, outPath);
        FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
        if (dfs.exists(outPath)) {
            dfs.delete(outPath, true);
        }

        try {
            job.waitForCompletion(true);

        } catch (InterruptedException ex) {
            Logger.getLogger(XmlDriver.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(XmlDriver.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}
