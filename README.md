build:
mvn clean package

step 0 is to pre-process the training set

java -cp target/netflix-prize-0.0.1-SNAPSHOT-with-dependencies.jar com.zhentao.netflix.prize.preprocess.Concat ~/netflix/download/training_set /tmp/all.txt

step 1 is to concatenate movie title and year to all ratings

hadoop jar target/netflix-prize-0.0.1-SNAPSHOT-with-dependencies.jar com.zhentao.netflix.prize.step1.Step1Driver movie_titles.txt concat-training-set.txt  all-record

-- input output minimum-ranking (R)
step 2 is to find the average rating for all movies with at least R ranking
hadoop jar target/netflix-prize-0.0.1-SNAPSHOT-with-dependencies.jar com.zhentao.netflix.prize.step2.Step2Driver all-record average-output 100

-- input out topN (M)
step 3 is to find M movies with highest ratings across all users (with at least R ratings)

hadoop jar target/netflix-prize-0.0.1-SNAPSHOT-with-dependencies.jar com.zhentao.netflix.prize.step3.Step3Driver average-output top-output 5

step3a
hadoop fs -getmerge top-output top-output-concat.txt


step 4 is to find all users and ratings who rated all M movies

hadoop jar target/netflix-prize-0.0.1-SNAPSHOT-with-dependencies.jar com.zhentao.netflix.prize.step4.Step4Driver -files top-output-concat.txt all-record all-user top-output-concat.txt

-- input out topN (U)
step 5 is to find contrarian users

hadoop jar target/netflix-prize-0.0.1-SNAPSHOT-with-dependencies.jar com.zhentao.netflix.prize.step5.Step5Driver all-user top-user 50

-- step 6 to find the final result
hadoop jar target/netflix-prize-0.0.1-SNAPSHOT-with-dependencies.jar com.zhentao.netflix.prize.step6.Step6Driver -files ../netflix-data/top-user/part-r-00000 ../netflix-data/all-record ../netflix-data/final ../netflix-data/top-user/part-r-00000
