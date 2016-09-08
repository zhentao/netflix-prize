package com.zhentao.netflix.prize.step1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.zhentao.netflix.prize.TextPair;

/**
 *
 * @author zhentao.li
 *
 */
public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {

    /**
     * Generate the new output for other jobs to use. The output is:
     *
     * <pre>
     * movieId,title,yearOfRelease  customerId,rating,ratingDate
     * </pre>
     */
    @Override
    protected void reduce(TextPair key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        Text yearAndTitle = new Text(iter.next());
        while (iter.hasNext()) {
            Text ratingRecord = iter.next();
            context.write(new Text(key.getFirst().toString() + "," + yearAndTitle.toString()), ratingRecord);
        }
    }
}
