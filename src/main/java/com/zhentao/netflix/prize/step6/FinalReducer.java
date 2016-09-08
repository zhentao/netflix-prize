package com.zhentao.netflix.prize.step6;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinalReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        FinalResult highest = new FinalResult(0, "", "", "");
        for (Text value : values) {
            String[] tokens = value.toString().split(",");
            String title = tokens[0];
            String yearOfRelease = tokens[1];
            String ratingDate = tokens[2];
            double rating = Double.parseDouble(tokens[3]);
            FinalResult current = new FinalResult(rating, title, yearOfRelease, ratingDate);
            if (current.compareTo(highest) > 0) {
                highest = current;
            }
        }
        context.write(key, new Text(highest.toString()));
    }
    static class FinalResult implements Comparable<FinalResult> {
        public FinalResult(double rating, String title, String yearOfRelease, String ratingDate) {
            super();
            this.rating = rating;
            this.title = title;
            this.yearOfRelease = yearOfRelease;
            this.ratingDate = ratingDate;
        }

        double rating;
        String title;
        String yearOfRelease;
        String ratingDate;

        @Override
        public int compareTo(FinalResult o) {
            if (rating > o.rating) {
                return 1;
            } else if (rating < o.rating) {
                return -1;
            } else {
                if (yearOfRelease.compareTo(o.yearOfRelease) > 0) {
                    return 1;
                } else if (yearOfRelease.compareTo(o.yearOfRelease) < 0) {
                    return -1;
                } else {
                    return -(title.compareTo(o.title));
                }
            }
        }

        @Override
        public String toString() {
            return title + "," + yearOfRelease + "," + ratingDate + "," + rating;
        }
    }
}
