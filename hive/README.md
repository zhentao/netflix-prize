How to run:

0. Build jar file
mvn clean package

1. pre-process the training set
java -cp target/netflix-prize-0.0.1-SNAPSHOT-with-dependencies.jar com.zhentao.netflix.prize.preprocess.Concat ~/netflix/download/training_set /tmp/all.txt

2. run hive 
(assume run-hive.sh and movie.hql are in the same foler)

./run-hive.sh 10 100 10 /tmp/movie_titles.txt /tmp/all.txt 
 