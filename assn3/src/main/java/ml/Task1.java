package ml;

import java.util.Collections;
import java.util.LinkedList;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.operators.Order;

public class Task1 {
	public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Which directory are we receiving input from?
        // This can be local or on HDFS; just format the path correctly for your OS.
        String fileDir = params.getRequired("file-dir");
        if(fileDir.charAt(fileDir.length() - 1) != '/') {
            fileDir = fileDir + '/';
        }

        // Read in genes file
        DataSet<String> genesRaw = env.readTextFile(fileDir + "GEO.txt");

        // Filter out the first line and patientID with gene 42 strongly expressed
        DataSet<String> filteredGenes = genesRaw.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) {
                String[] arguments = value.split(",");
                String patientID = arguments[0];
                // Filter out the first line
                if (patientID.equals("patientid")) {
                    return false;
                }
                int geneID = Integer.parseInt(arguments[1]);
                double expression = Double.parseDouble(arguments[2]);
                // Filter out genes with invalid ID
                if (geneID != 42) {
                    return false;
                }
                // Filter out inactive genes
                if (expression < 1250000) {
                    return false;
                }
                return true;
            }
        });

        // Map the filtered result
        // Output format: 
        // (patientid, 1)
        DataSet<Tuple2<String, Integer>> validPatientIDs =
            filteredGenes
                .map(line -> new Tuple2<String, Integer>(line.split(",")[0], 1));

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
        // (id, 1), (id, disease) -> (id, disease)
        DataSet<Tuple2<String, String>> joinedTable =
            validPatientIDs
                .join(filteredPatients)
                .where(0)
                .equalTo(0)
                .projectFirst(0)
                .projectSecond(1);

        // Map the dataset to
        // (disease, diseaseCount)
        DataSet<Tuple2<String,Integer>> reversedTable =
            joinedTable.map(s -> new Tuple2<String, Integer>(s.f1, 1))
                .groupBy(0)
                .sum(1);

        // Sort mapped dataset
        DataSet<Tuple2<String,Integer>> sortedTable =
            reversedTable.sortPartition(1, Order.DESCENDING).setParallelism(1);

        // Result for output
        DataSet<String> result = sortedTable.map(s -> new String(s.f0 + "\t" + s.f1.toString()));

        // End the program by writing the output!
        if(params.has("output")) {
            result.writeAsText(params.get("output"));
            env.execute();
        } else {
            // Always limit direct printing
            // as it requires pooling all resources into the driver.
            System.err.println("No output location specified; printing first 100.");
            result.first(100).print();
        }
	}
}
