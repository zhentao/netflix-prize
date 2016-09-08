package com.zhentao.netflix.prize.step5;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.zhentao.netflix.prize.UserRating;

public class TopUserMapper extends Mapper<Text, Text, NullWritable, Text> {
    private long topNUser;
    private TreeSet<UserRating> topNSet = new TreeSet<>();

    @Override
    public void setup(Context context) throws IOException {
        topNUser = Long.parseLong(context.getConfiguration().get("top.user"));
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        double rating = Double.parseDouble(value.toString());
        long customerId = Long.parseLong(key.toString());
        topNSet.add(new UserRating(customerId, rating));
        if (topNSet.size() > topNUser) {
            topNSet.pollLast();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (UserRating userRating : topNSet) {

            context.write(NullWritable.get(), new Text(userRating.toString()));
        }
    }
}