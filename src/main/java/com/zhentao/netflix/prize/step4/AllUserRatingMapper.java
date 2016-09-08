package com.zhentao.netflix.prize.step4;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.zhentao.netflix.prize.Movie;

public class AllUserRatingMapper extends Mapper<Text, Text, LongWritable, DoubleWritable> {
    private HashMap<String, Movie> map = new HashMap<>();
    private String movieFile;
    @Override
    public void setup(Context context) throws IOException {
        movieFile = context.getConfiguration().get("movie.file");
        map = createMap(movieFile);
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        Movie movie = Movie.parse(key.toString());
        String[] tokens = value.toString().split(",");
        Long customerId = Long.parseLong(tokens[0]);
        double rating = Double.parseDouble(tokens[1]);
        if (map.containsKey(movie.getId())) {
            context.write(new LongWritable(customerId), new DoubleWritable(rating));
        }
    }

    static HashMap<String, Movie> createMap(String file) throws IOException {
        HashMap<String, Movie> map = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String key = line.split("\t")[0];
                String[] tokens = key.split(",");
                String movieId = tokens[0];
                String year = tokens[1];
                String title = tokens[2];
                map.put(movieId, new Movie(movieId, year, title));
            }
            return map;
        }
    }
}
