package com.zhentao.netflix.prize.step2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageRatingReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private int minNumberRanking;

    @Override
    public void setup(Context context) {
        minNumberRanking = Integer.parseInt(context.getConfiguration().get("min.number.ranking"));
    }

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        int countOfRanking = 0;
        double total = 0;
        for (DoubleWritable ranking : values) {
            countOfRanking++;
            total += ranking.get();
        }
        if (countOfRanking >= minNumberRanking) {
            context.write(key, new DoubleWritable(total / countOfRanking));
        }
    }
}
