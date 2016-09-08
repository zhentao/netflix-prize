package com.zhentao.netflix.prize.step5;

import java.util.TreeSet;

import org.junit.Test;

import com.zhentao.netflix.prize.UserRating;

public class TreeSetTest {
    @Test
    public void pollLastTest() {
        TreeSet<UserRating> topNSet = new TreeSet<>();
        UserRating rating = new UserRating(1, 5.0);
        topNSet.add(rating);
        rating = new UserRating(2, 5.0);
        topNSet.add(rating);
        rating = new UserRating(3, 3.0);
        topNSet.add(rating);

        for (UserRating userRating : topNSet) {
            System.out.println(userRating);
        }

        topNSet.pollLast();
        System.out.println("after poll last");
        for (UserRating userRating : topNSet) {
            System.out.println(userRating);
        }

    }
}
