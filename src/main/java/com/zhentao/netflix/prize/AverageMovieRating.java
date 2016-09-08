package com.zhentao.netflix.prize;

public class AverageMovieRating implements Comparable<AverageMovieRating> {
    private Movie movie;
    private double rating;
    public AverageMovieRating(Movie movie, double rating) {
        super();
        this.movie = movie;
        this.rating = rating;
    }
    public Movie getMovie() {
        return movie;
    }
    public double getRating() {
        return rating;
    }
    @Override
    public String toString() {
        return movie.toString() + "," + rating;
    }
    @Override
    public int compareTo(AverageMovieRating other) {
        if (rating > other.rating) {
            return 1;
        } else if (rating < other.rating) {
            return -1;
        } else {
            return movie.compareTo(other.movie);
        }
    }
    /**
     * input format:
     *
     * <pre>
     * movieId,year,title,rating
     * </pre>
     *
     * @param input
     * @return
     */
    public static AverageMovieRating parse(String input) {
        try {
            String[] tokens = input.split(",");
            Movie movie = new Movie(tokens[0], tokens[1], tokens[2]);
            return new AverageMovieRating(movie, Double.parseDouble(tokens[3]));
        } catch (Exception e) {
            throw new IllegalArgumentException(input + " is invalid", e);
        }
    }
}
