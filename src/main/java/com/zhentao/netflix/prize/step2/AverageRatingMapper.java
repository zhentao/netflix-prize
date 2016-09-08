package com.zhentao.netflix.prize.step2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AverageRatingMapper extends Mapper<Text, Text, Text, DoubleWritable> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        double rating = Double.parseDouble(value.toString().split(",")[1]);
        context.write(key, new DoubleWritable(rating));
    }
}
