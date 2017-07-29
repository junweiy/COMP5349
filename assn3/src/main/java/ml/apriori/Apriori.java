package ml.apriori;

import java.util.ArrayList;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 *
 * Apriori algorithm implemented in Flink.
 *
 */
public class Apriori {

	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.getConfig().setGlobalJobParameters(params); // make parameters available in the web interface

		double minSupport = params.getDouble("min-support", 0.5);
		int iterations = params.getInt("itemset-size", 4);

		if (!parametersCorrect(minSupport, iterations)) { return; }

		// Which directory are we receiving input from?
        // This can be local or on HDFS; just format the path correctly for your OS.
        String fileDir = params.getRequired("file-dir");
        if (fileDir.charAt(fileDir.length() - 1) != '/') {
            fileDir = fileDir + '/';
        }

        // Read in genes file
        DataSet<String> genesRaw = env.readTextFile(fileDir + "GEO.txt");

        // Filter out the first line and patientID with gene strongly expressed
        DataSet<String> filteredGenes = genesRaw.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) {
                String[] arguments = value.split(",");
                String patientID = arguments[0];
                // Filter out the first line
                if (patientID.equals("patientid")) {
                    return false;
                }
                double expression = Double.parseDouble(arguments[2]);
                // Filter out inactive genes
                if (expression < 1250000) {
                    return false;
                }
                return true;
            }
        });

        // Map the filtered result
        // Output format: 
        // (patientid, geneID)
        DataSet<Tuple2<String, String>> validCells =
            filteredGenes
                .map(s -> {
                    String[] values = s.split(",");
                    return new Tuple2<String, String>(values[0],values[1]);
                });

        // Read in patients file
        DataSet<String> patientsRaw = env.readTextFile(fileDir + "PatientMetaData.txt");

        // flatMap and filter the patients table with required cancer type
        // (id, age, gender, postcode, disaeases, drug_response) -> (id, disease)
        DataSet<Tuple2<String, String>> filteredPatients =
            patientsRaw.flatMap((line, out) -> {
                String[] values = line.split(",");
                String patientID = values[0];
                String[] diseases = values[4].split(" ");
                for (String disease: diseases) {
                    if (disease.equals("breast-cancer") || disease.equals("prostate-cancer") || 
                        disease.equals("pancreatic-cancer") || disease.equals("leukemia") ||
                        disease.equals("lymphoma")) {
                        out.collect(new Tuple2<String, String>(patientID, disease));    
                    }
                }
            });

        // Join two datasets
        // (patientid, geneID), (id, disease) -> (patientid, geneID)
        DataSet<Tuple2<String, String>> input =
            validCells
                .join(filteredPatients)
                .where(0)
                .equalTo(0)
                .projectFirst(0,1);

		// get the number of distinct transactions
		long numberOfTransactions = input
				.distinct(0)
				.count();
		// calculate the number of transactions sufficient for the support threshold
		long minNumberOfTransactions = (long) (numberOfTransactions * minSupport);

		DataSet<Tuple2<String, ArrayList<Integer>>> transactions = input
				.groupBy(0)
				.reduceGroup(new TransactionGroupReduceFunction());

		// compute frequent itemsets for itemset_size = 1
		DataSet<ItemSet> c1 = input
				// map item to 1
				.map(new InputMapFunction())
				// group by hashCode of the ItemSet
				.groupBy(new ItemSetKeySelector())
				// sum the number of transactions containing the ItemSet
				.reduce(new ItemSetReduceFunction())
				// remove ItemSets with frequency under the support threshold
				.filter(new ItemSetFrequencyFilterFunction(minNumberOfTransactions));

		// start of the loop
		// itemset_size = 2
		IterativeDataSet<ItemSet> initial = c1.iterate(iterations - 1);

		// create the candidate itemset for the next iteration
		DataSet<ItemSet> candidates = initial.cross(c1)
				.with(new ItemSetCrossFunction())
				.distinct(new ItemSetKeySelector());

		// calculate actual numberOfTransactions
		DataSet<ItemSet> selected = candidates
				.map(new ItemSetCalculateFrequency()).withBroadcastSet(transactions, "transactions")
				.filter(new ItemSetFrequencyFilterFunction(minNumberOfTransactions));

		// end of the loop
		// stop when we run out of iterations or candidates is empty
		DataSet<ItemSet> output = initial.closeWith(selected, selected);

		if (params.has("output")) {
			// write the final solution to file
			//output.writeAsFormattedText(params.get("output"), new ItemSetTextFormatter());
			env.execute("Flink Apriori");
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			output.print();

			System.out.println("Number of iterations: " + iterations);
			System.out.println("Number of transactions: " + numberOfTransactions);

			System.out.println("Minimal number of transactions for support threshold of "
					+ minSupport + " = " + minNumberOfTransactions);
		}
	}

	// check parameters
	private static boolean parametersCorrect(double support_threshold, int iterations) {
		boolean paremeters_correct = true;

		if (support_threshold <= 0) {
			System.out.println("Incorrect parameter '--min-support'.\n"
					+ "The support threshold must be > 0.\n"
					+ "Use a value in the range (0,1].");
			paremeters_correct = false;
		} else if (support_threshold > 1) {
			System.out.println("Incorrect parameter '--min-support'.\n"
					+ "The support threshold must be <= 1.\n"
					+ "Use a value in the range (0,1].");
			paremeters_correct = false;
		}

		if (iterations < 1) {
			System.out.println("Incorrect parameter '--itemset-size'.\n"
					+ "The size of the itemsets must be >= 1.\n"
					+ "Use a value in the range (1, Infinity]");
			paremeters_correct = false;
		}

		return paremeters_correct;
	}

}
