package com.zhentao.netflix.prize.step4;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AllUserRatingReducer extends Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {
    private long topNMovie;

    private String movieFile;
    @Override
    public void setup(Context context) throws IOException {
        movieFile = context.getConfiguration().get("movie.file");
        topNMovie = count(movieFile);
    }

    static long count(String movieFile) throws IOException {
        Path path = Paths.get(movieFile);
        return Files.lines(path).count();
    }

    @Override
    protected void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        double total = 0;
        for (DoubleWritable value : values) {
            count++;
            total += value.get();
        }

        if (count == topNMovie) {
            context.write(key, new DoubleWritable(total / count));
        }
    }
}
