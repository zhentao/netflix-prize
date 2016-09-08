package com.zhentao.netflix.prize.step3;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.zhentao.netflix.prize.AverageMovieRating;
import com.zhentao.netflix.prize.Movie;

public class TopMovieMapper extends Mapper<Text, Text, NullWritable, Text> {
    private int topN;
    private TreeSet<AverageMovieRating> topNSet = new TreeSet<>();

    @Override
    public void setup(Context context) {
        topN = Integer.parseInt(context.getConfiguration().get("topN"));
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        double rating = Double.parseDouble(value.toString());
        Movie movie = Movie.parse(key.toString());
        AverageMovieRating averageMovieRating = new AverageMovieRating(movie, rating);
        topNSet.add(averageMovieRating);
        if (topNSet.size() > topN) {
            topNSet.pollFirst();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (AverageMovieRating average : topNSet) {
            context.write(NullWritable.get(), new Text(average.toString()));
        }
    }
}
