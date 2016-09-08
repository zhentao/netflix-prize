package com.zhentao.netflix.prize;

public class MovieRating {
    private String movieId;
    private long customerId;
    private double rating;
    private String ratingDate;

    public MovieRating(String movieId, long customerId, double rating, String ratingDate) {
        super();
        this.movieId = movieId;
        this.customerId = customerId;
        this.rating = rating;
        this.ratingDate = ratingDate;
    }

    public String getMovieId() {
        return movieId;
    }

    public long getCustomerId() {
        return customerId;
    }

    public double getRating() {
        return rating;
    }

    public String getRatingDate() {
        return ratingDate;
    }

    public static MovieRating parse(String input) {
        try {
            String[] tokens = input.split(",");
            String movieId = tokens[0];
            long customerId = Long.parseLong(tokens[1]);
            double rating = Double.parseDouble(tokens[2]);
            return new MovieRating(movieId, customerId, rating, tokens[3]);
        } catch (Exception e) {
            throw new IllegalArgumentException(input + " is invalid", e);
        }
    }

    public String toRatingString() {
        return customerId + "," + rating + "," + ratingDate;
    }
}
