package com.zhentao.netflix.prize.step6;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.zhentao.netflix.prize.Movie;

public class FinalMapper extends Mapper<Text, Text, Text, Text> {
    private Set<String> contrarianUser;
    private String contrarianFile;
    @Override
    public void setup(Context context) throws IOException {
        contrarianFile = context.getConfiguration().get("contrarian.file");
        contrarianUser = createSet(contrarianFile);
    }
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        Movie movie = Movie.parse(key.toString());
        String[] tokens = value.toString().split(",");
        String customerId = tokens[0];
        double rating = Double.parseDouble(tokens[1]);
        String ratingDate = tokens[2];

        if (contrarianUser.contains(customerId)) {
            StringBuilder builder = new StringBuilder(movie.getTitle()).append(",").append(movie.getYearOfRelease())
                    .append(",").append(ratingDate).append(",").append(rating);
            context.write(new Text(customerId), new Text(builder.toString()));
        }
    }

    static Set<String> createSet(String file) throws IOException {
        try (Stream<String> stream = Files.lines(Paths.get(file))) {

            // 1. filter line 3
            // 2. convert all content to upper case
            // 3. convert it into a List
            // return stream.filter(line ->
            // !line.startsWith("line3")).map(String::toUpperCase).collect(Collectors.toSet());
            return stream.map(line -> line.split("\t")[0]).collect(Collectors.toSet());

        }
    }
}
