package ml;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Arrays;
import java.util.List;
import java.util.LinkedList;

import org.apache.flink.api.java.io.TextOutputFormat.TextFormatter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.aggregators.ConvergenceCriterion;


public class Task3 {
	public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        double confidenceValue = 0.6;
        int maxItemSize = Integer.MAX_VALUE;

        // Which directory are we receiving input from?
        // This can be local or on HDFS; just format the path correctly for your OS.
        String fileDir = params.getRequired("file-dir");
        String resultDir = params.getRequired("result-dir");
        if (fileDir.charAt(fileDir.length() - 1) != '/') {
            fileDir = fileDir + '/';
        }
        if (resultDir.charAt(resultDir.length() - 1) != '/') {
            resultDir = resultDir + '/';
        }
        if (params.has("confidence")) {
            confidenceValue = Double.parseDouble(params.get("confidence"));
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
        DataSet<Tuple2<String, String>> joinedTable =
            validCells
                .join(filteredPatients)
                .where(0)
                .equalTo(0)
                .projectFirst(0,1);

        // Transaction
        // (<patientID, [geneID0, geneID1, ...]>)
        DataSet<Tuple2<String, List<String>>> transactions = joinedTable
            .groupBy(0)
            .reduceGroup((tuples, out) -> {
                String id = "";
                List<String> tempList = new LinkedList<String>();
                for (Tuple2<String, String> tuple: tuples) {
                    id = tuple.f0;
                    if (!tempList.contains(tuple.f1)) {
                        tempList.add(tuple.f1);
                    }
                }
                out.collect(new Tuple2<String, List<String>>(id, tempList));
            });

        // Read in the support information from the previous task
        DataSet<String> supportRaw = env.readTextFile(resultDir);

        // Store the support value and item sets
        DataSet<Tuple2<List<String>, Integer>> supportItem= supportRaw.map(s -> {
            String[] values = s.split("\t");
            int support = Integer.parseInt(values[0]);
            List<String> tempList = new LinkedList<String>();
            for (int i = 1; i < values.length; i++){
              tempList.add(values[i]);
            }
            return new Tuple2<List<String>, Integer>(tempList, support);
        });

        System.out.println("*********");
        // supportItem.print();

        //Find all possible subsets for each item sets
        // Foramt: <<[gene0, gene1,..], support> , [gene0],[gene1],..>
        DataSet<Tuple2<Tuple2<List<String>, Integer>, List<List<String>>>> supportItemSubset = supportItem.map(s -> {
          List<String> itemSet = s.f0;
          List<List<String>> subsets = getSubsets(itemSet);
          subsets.remove(itemSet);
          return new Tuple2<Tuple2<List<String>, Integer>, List<List<String>>>(s, subsets);
        });
				
        // Flat map the subsets
        // Format: <<[gene0, gene1,..], support> , [gene0]>
        DataSet<Tuple2<Tuple2<List<String>, Integer>, List<String>>> itemSubsetSupport = supportItemSubset.flatMap((s, out) -> {
          List<List<String>> subsets = s.f1;
          for (List<String> sub: subsets){
            out.collect(new Tuple2<Tuple2<List<String>, Integer>, List<String>>(s.f0, sub));
          }
        });

        // Count the support value for each subset and calculate the confidence
        // Format: [gene0, gene1], <[gene0], confidence>
        DataSet<Tuple2<List<String>, Tuple2<List<String>, Double>>> itemConfidence = itemSubsetSupport
            .map(new CountSubsetRichMapFunction())
            .withBroadcastSet(transactions, "transactions")
            .filter(new CountSubsetFilterFunction(confidenceValue));

        // itemConfidence.print();

        // Sort the output by DESCENDING order of confidenceValue
        // Format: <[gene0, gene1], [gene0]>, confidence
        DataSet<Tuple2<Tuple2<List<String>, List<String>>, Double>> confidenceResult = itemConfidence
            .map(s -> {
              return new Tuple2<Tuple2<List<String>, List<String>>, Double>(
                  new Tuple2<List<String>, List<String>>(s.f0, s.f1.f0),
                  s.f1.f1
              );
            }).sortPartition(1, Order.DESCENDING).setParallelism(1);

        // confidenceResult.print();

        // Write the output to the specified output location
        if(params.has("output")) {
            confidenceResult.writeAsFormattedText(params.get("output"), new ResultTextFormatter());
            env.execute();
        } else {
            // Always limit direct printing
            // as it requires pooling all resources into the driver.
            System.err.println("No output location specified; printing first 100.");
            confidenceResult.first(100).print();
        }
  }

  /** Generate all non-empty subsets of the list
      The return type of this helper function is a list of list of string where
      each list of string represents a sub item set
      This function is implemented in a recursive way
  **/
  public static List<List<String>> getSubsets(List<String> list){
      List<List<String>> collections = new LinkedList<List<String>>();
      if(list.isEmpty()){
        return collections;
      } else {
        String first = list.get(0);
        List<String> firstList = new LinkedList<String>();
        firstList.add(first);
        for(List<String> sublist: getSubsets(list.subList(1, list.size()))) {
          List<String> sublistWithFirst = new LinkedList<String>();
          sublistWithFirst.add(first);
          sublistWithFirst.addAll(sublist);
          collections.add(sublist);
          collections.add(sublistWithFirst);
        }
        collections.add(firstList);
      }
      return collections;
  }

  /**
     This is the helper class inherited from the RichMapFunction.
     This class is used to define the method for calculating confidence value for each subsets of genes.
  **/
  public static class CountSubsetRichMapFunction extends RichMapFunction<Tuple2<Tuple2<List<String>, Integer>, List<String>>
                                                , Tuple2<List<String>, Tuple2<List<String>, Double>>> {
      private List<Tuple2<String, List<String>>> transactions;
      @Override
      public void open(Configuration parameters) throws Exception {
          this.transactions = getRuntimeContext().
          getBroadcastVariable ("transactions");
      }

      // The map function calculates the support value for subset then calculate the confidence value
      @Override
      public Tuple2<List<String>, Tuple2<List<String>, Double>> map(Tuple2<Tuple2<List<String>, Integer>, List<String>> value) {
          List<String> subsets = value.f1;
          Tuple2<List<String>,Integer> supportResult = countItem(subsets, transactions);
          double supportSub = (double) supportResult.f1;
          double supportOrg = (double) value.f0.f1;
          double confidence = supportOrg/supportSub;
          return new Tuple2<List<String>, Tuple2<List<String>, Double>>(value.f0.f0, new Tuple2<List<String>, Double>(subsets, confidence));
      }
  }

	// Count the number of genes with arbitrary size in each transaction
	// Input format:
	// genes [geneID1, geneID2, ...], trans [<patient0, [geneID1, ...]>, ...]
	// Output format:
	// <[geneID1, geneID2, ...], count>
	public static Tuple2<List<String>,Integer> countItem(List<String> genes, List<Tuple2<String, List<String>>> trans) {
			int count = 0;
			for (Tuple2<String, List<String>> tran: trans) {
					if (tran.f1.containsAll(genes)) {
							count += 1;
					}
			}
			return new Tuple2<List<String>,Integer>(genes, count);
	}

  /**
    This helper class inherited from FilterFunction.
    This helper defines filter method for filtering subsets with confidence value below the threshold
  **/
  public static class CountSubsetFilterFunction implements FilterFunction<Tuple2<List<String>, Tuple2<List<String>, Double>>> {
      final private double thres;

      public CountSubsetFilterFunction(double thres) {
          this.thres = thres;
      }

      @Override
      public boolean filter(Tuple2<List<String>, Tuple2<List<String>, Double>> value) {
          return value.f1.f1 >= thres;
      }
  }

  /**
     This is the helper function for formatting the result dataset to the required format to write to the file
  **/
  public static class ResultTextFormatter implements TextFormatter<Tuple2<Tuple2<List<String>, List<String>>, Double>>{
      @Override
      public String format(Tuple2<Tuple2<List<String>, List<String>>, Double> value) {
          String output = "";
          for (String st: value.f0.f1) {
              output += (st + " ");
          }
          output += "\t";
          for (String st: value.f0.f0) {
              if(!value.f0.f1.contains(st)){
                output += (st + " ");
              }
          }
          output += ("\t" + value.f1.toString());
          return output;
      }
  }

}
