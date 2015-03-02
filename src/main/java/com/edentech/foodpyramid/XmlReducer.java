package com.edentech.foodpyramid;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class XmlReducer extends
        Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String outVals = "";
        for (Text val : values) {
            outVals += " " + val.toString();
        }
        context.write(key, new Text(outVals));
    }
}
