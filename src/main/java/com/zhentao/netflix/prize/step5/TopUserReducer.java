package com.zhentao.netflix.prize.step5;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.TreeSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.zhentao.netflix.prize.UserRating;

public class TopUserReducer extends Reducer<NullWritable, Text, LongWritable, DoubleWritable> {
    private long topNUser;
    private TreeSet<UserRating> topNSet = new TreeSet<>();

    @Override
    public void setup(Context context) throws IOException {
        topNUser = Long.parseLong(context.getConfiguration().get("top.user"));
    }

    static long count(String movieFile) throws IOException {
        Path path = Paths.get(movieFile);
        return Files.lines(path).count();
    }

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            String[] tokens = value.toString().split(",");
            double rating = Double.parseDouble(tokens[1]);
            long customerId = Long.parseLong(tokens[0]);
            topNSet.add(new UserRating(customerId, rating));
            if (topNSet.size() > topNUser) {
                topNSet.pollLast();
            }
        }
        for (UserRating userRating : topNSet) {
            context.write(new LongWritable(userRating.getCustomerId()), new DoubleWritable(userRating.getRating()));
        }
    }
}