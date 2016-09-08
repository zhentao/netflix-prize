package com.zhentao.netflix.prize.step1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhentao.netflix.prize.Movie;
import com.zhentao.netflix.prize.TextPair;

public class JoinMovieTitleMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(JoinMovieTitleMapper.class);

    public enum Input {
        INVALID;
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            Movie movie = Movie.parse(value.toString());
            context.write(new TextPair(movie.getId(), "0"), new Text(getTitleAndYear(movie)));
        } catch (IllegalArgumentException e) {
            LOG.error("failed to parse movie", e);
            context.getCounter(Input.INVALID).increment(1);
        }
    }

    private String getTitleAndYear(Movie movie) {
        return movie.getYearOfRelease() + "," + movie.getTitle();
    }
}
