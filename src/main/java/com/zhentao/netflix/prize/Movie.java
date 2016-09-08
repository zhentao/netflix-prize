package com.zhentao.netflix.prize;

public class Movie implements Comparable<Movie>{
    private String id;
    private String title;
    private String yearOfRelease;

    public Movie(String id, String yearOfRelease, String title) {
        this.id = id;
        this.title = title;
        this.yearOfRelease = yearOfRelease;
    }

    public String getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public String getYearOfRelease() {
        return yearOfRelease;
    }

    /**
     * The format of input is:
     *
     * <pre>
     * id,yearOfRelease,title
     *
     * for example:
     * 1,2003,Dinosaur Planet
     * </pre>
     *
     * @param line
     *            movie info
     * @return
     */
    public static Movie parse(String line) {
        try {
            String[] tokens = line.split(",");
            return new Movie(tokens[0], tokens[1], tokens[2]);
        } catch (Exception e) {
            throw new IllegalArgumentException(line + " is invalid", e);
        }
    }

    @Override
    public int compareTo(Movie other) {
        int value = yearOfRelease.compareTo(other.yearOfRelease);
        if (value != 0) {
            return value;
        }

        return -title.compareTo(other.title);
    }

    @Override
    public String toString() {
        return id + "," + yearOfRelease + "," + title;
    }
}
