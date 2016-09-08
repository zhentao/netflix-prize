package com.zhentao.netflix.prize.preprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * A utility class to convert multiple train set files to one file with the following format
 * <pre>
 * movie-id:customer-id,rating,rating-date
 *
 * 1,1488844,3,2005-09-06
 * </pre>
 * @author zhentao.li
 *
 */
public class Concat {
    public static void main(String[] args) throws IOException {
        String inputFolder = args[0];
        String outputFile = args[1];

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(inputFolder))) {
                for (Path entry : stream) {
                    try (BufferedReader reader = Files.newBufferedReader(entry)) {
                        String movieId = reader.readLine().trim().replace(":", ",");
                        String line;
                        while ((line = reader.readLine()) != null) {
                            writer.write(movieId + line);
                            writer.newLine();
                        }
                    }
                }
            }
        }
    }
}
