package com.zhentao.netflix.prize;

public class UserRating implements Comparable<UserRating>{
    private long customerId;
    private double rating;

    public UserRating(long customerId, double rating) {
        this.customerId = customerId;
        this.rating = rating;
    }

    public long getCustomerId() {
        return customerId;
    }

    public double getRating() {
        return rating;
    }

    @Override
    public int compareTo(UserRating other) {
        if (rating > other.rating) {
            return 1;
        } else if (rating < other.rating) {
            return -1;
        } else {
            if (customerId > other.customerId) {
                return 1;
            } else if (customerId < other .customerId) {
                return -1;
            }
            return 0;
        }
    }

    @Override
    public String toString() {
        return customerId + "," + rating;
    }
}
