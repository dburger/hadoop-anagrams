package com.dburger.hadoop.anagram;

import java.io.IOException;

import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class AnagramList {

    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {super(Text.class);}
        public String toString() {
            return Arrays.toString(toStrings());
        }
    }

    public static class Map extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, TextArrayWritable> {
        private final static TextArrayWritable writable
                = new TextArrayWritable();
        private final static Writable[] writables = new Writable[1];
        private Text newKey = new Text();

        public void map(LongWritable key, Text value,
                OutputCollector<Text, TextArrayWritable> output,
                Reporter reporter) throws IOException {
            String word = value.toString();
            char[] chars = word.toCharArray();
            Arrays.sort(chars);
            newKey.set(new String(chars));
            writables[0] = new Text(word);
            writable.set(writables);
            output.collect(newKey, writable);
        }
    }

    public static class Reduce extends MapReduceBase
            implements Reducer<Text, TextArrayWritable, Text,
            TextArrayWritable> {
        public void reduce(Text key, Iterator<TextArrayWritable> values,
                OutputCollector<Text, TextArrayWritable> output,
                Reporter reporter) throws IOException {
            List<Writable> list = new ArrayList<Writable>();
            while (values.hasNext()) {
                Writable[] writables = values.next().get();
                for (Writable t: writables) {list.add(t);}
            }
            Writable[] writables = list.toArray(new Writable[0]);
            TextArrayWritable writable = new TextArrayWritable();
            writable.set(writables);
            output.collect(key, writable);
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(AnagramList.class);
        conf.setJobName("anagramlist");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(TextArrayWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
