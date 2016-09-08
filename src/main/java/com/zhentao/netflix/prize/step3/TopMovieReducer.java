package com.zhentao.netflix.prize.step3;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.zhentao.netflix.prize.AverageMovieRating;

public class TopMovieReducer extends Reducer<NullWritable, Text, Text, Text> {
    private int topN;
    private TreeSet<AverageMovieRating> topNSet = new TreeSet<>();

    @Override
    public void setup(Context context) {
        topN = Integer.parseInt(context.getConfiguration().get("topN"));
    }

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            AverageMovieRating average = AverageMovieRating.parse(value.toString());
            topNSet.add(average);

            if (topNSet.size() > topN) {
                topNSet.pollFirst();
            }
        }

        for (AverageMovieRating average : topNSet.descendingSet()) {
            context.write(new Text(average.getMovie().toString()), new Text(average.getRating() +""));
        }
    }
}
