package ml;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.configuration.GlobalConfiguration;

public class MovieLensGenreAverageRating {
	public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Which directory are we receiving input from?
        // This can be local or on HDFS; just format the path correctly for your OS.
        String movieDir = params.getRequired("movie-dir");
        if(movieDir.charAt(movieDir.length() - 1) != '/') {
            movieDir = movieDir + '/';
        }

        // Read in the ratings CSV file, whose initial format is:
        // (movieId, userId, rating, timestamp)
        //
        // We're only interested in:
        // (movieId, rating)
        DataSet<Tuple2<String, Double>> ratings =
            env.readCsvFile(movieDir + "ratings.csv")
                .includeFields("1010")
                .types(String.class, Double.class);

        // Read in the movies CSV file, whose initial format is:
        // (movieId, name, genreList)
        //
        // We'll split the genre list into individual genres using flat map:
        // (movieId, genre)
        //
        // We can't use CSV processing for the movies because of this:
        // http://stackoverflow.com/q/38300903
        DataSet<Tuple2<String, String>> movieGenre =
            env.readTextFile(movieDir + "movies.csv")
                .flatMap((line, out) -> {
                    String[] values = line.split(",");

                    if(values.length >= 3) {
                        String movieId = values[0];
                        String[] genres = values[values.length - 1].split("\\|");

                        for(String genre : genres) {
                            out.collect(new Tuple2<String, String>(movieId, genre));
                        }
                    }
                });

        // Join using the movie ID, going from:
        // (movieId, genre), (movieId, rating)
        //
        // ... to:
        // (movieId, genre, rating)
        DataSet<Tuple3<String, String, Double>> movieGenreRating =
            movieGenre
                .join(ratings)
                .where(0)
                .equalTo(0)
                .projectFirst(0, 1)
                .projectSecond(1);

        // We don't actually need the movie ID anymore, so go to this:
        // (genre, rating)
        //
        // There are many better ways of doing this >.>
        DataSet<Tuple2<String, Double>> genreRating =
            movieGenreRating
                .map(tuple -> new Tuple2<String, Double>(tuple.f1, tuple.f2));

        // Finally use a group reducer to calculate the average for each genre:
        // (genre, averageRating)
        //
        // This is a verbose example and there are many different ways to do this.
        // Another more succinct example is demonstrated below.
        DataSet<Tuple2<String, Double>> genreAverageRating = 
            genreRating
                .groupBy(0)
                .sortGroup(0, Order.ASCENDING)
                .reduceGroup((tuples, out) -> {
                    String genre = "";
                    Double sum = 0d;
                    Double count = 0d;

                    for(Tuple2<String, Double> tuple : tuples) {
                        genre = tuple.f0;
                        sum += tuple.f1;
                        count += 1;
                    }

                    out.collect(new Tuple2<String, Double>(genre, sum / count));
                });

        /* The following alternative solution involves pre-made aggregates,
         * but requires an additional integer to tag along from earlier:
        DataSet<Tuple3<String, Double, Integer>> genreRatingCount =
            movieGenreRating
                .map(tuple -> new Tuple3<String, Double, Integer>(tuple.f1, tuple.f2, 1));

        DataSet<Tuple2<String, Double>> genreAverageRating =
            genreRatingCount
                .groupBy(0)
                .sum(1) // Sum of all ratings.
                .andSum(2) // Count all ratings.
                .map(tuple -> new Tuple2<String, Double>(tuple.f0, tuple.f1 * 1.0 / tuple.f2));
         */

        // End the program by writing the output!
        if(params.has("output")) {
            genreAverageRating.writeAsCsv(params.get("output"));

            env.execute();
        } else {
            // Always limit direct printing
            // as it requires pooling all resources into the driver.
            System.err.println("No output location specified; printing first 100.");
            genreAverageRating.first(100).print();
        }
	}
}
