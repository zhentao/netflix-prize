package com.zhentao.netflix.prize.step1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhentao.netflix.prize.MovieRating;
import com.zhentao.netflix.prize.TextPair;

public class JoinRatingMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(JoinRatingMapper.class);

    public enum Input {
        INVALID;
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            MovieRating movieRating= MovieRating.parse(value.toString());
            context.write(new TextPair(movieRating.getMovieId(), "1"), new Text(movieRating.toRatingString()));
        } catch (IllegalArgumentException e) {
            LOG.error("failed to parse movie", e);
            context.getCounter(Input.INVALID).increment(1);
        }
    }
}
