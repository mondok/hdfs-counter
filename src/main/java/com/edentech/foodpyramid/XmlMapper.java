package com.edentech.foodpyramid;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.logging.Level;
import java.util.logging.Logger;




public class XmlMapper extends
        Mapper<LongWritable, Text, Text, Text> {


    @Override
    public void map(LongWritable key, Text value1, Context context)

            throws IOException, InterruptedException {

        String xmlString = value1.toString();
        SAXBuilder builder = new SAXBuilder();
        Reader in = new StringReader(xmlString);
        String value = "";
        try {
            Document doc = builder.build(in);
            Element root = doc.getRootElement();
            String tag1 = root.getChild("Display_Name").getTextTrim();
            String tag2 = root.getChild("Portion_Display_Name").getTextTrim();
            value = tag1;
            context.write(new Text(value), new Text(tag2));
        } catch (JDOMException ex) {
            Logger.getLogger(XmlMapper.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(XmlMapper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }


}
