package ml;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.Arrays;
import java.util.List;
import java.util.LinkedList;
import java.util.Comparator;

import org.apache.flink.api.java.io.TextOutputFormat.TextFormatter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.operators.IterativeDataSet; 
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.aggregators.ConvergenceCriterion; 

public class Task2 {
	public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        double supportValue = 0.3;
        int maxItemSize = 10;

        // Which directory are we receiving input from?
        // This can be local or on HDFS; just format the path correctly for your OS.
        String fileDir = params.getRequired("file-dir");
        if (fileDir.charAt(fileDir.length() - 1) != '/') {
            fileDir = fileDir + '/';
        }

        if (params.has("support")) {
            supportValue = Double.parseDouble(params.get("support"));
        }

        if (params.has("maxsize")) {
            maxItemSize = Integer.parseInt(params.get("maxsize"));
        }

        // Read in genes file
        DataSet<String> genesRaw = env.readTextFile(fileDir + "GEO.txt");

        // Filter out the first line and patientID with gene strongly expressed
        DataSet<String> filteredGenes = genesRaw.filter(new RawGeneFilterFunction());

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
            patientsRaw.flatMap(new PatientsFlatMapFunction());

        // Join two datasets
        // (patientid, geneID), (id, disease) -> (patientid, geneID)
        DataSet<Tuple2<String, String>> joinedTable =
            validCells
                .join(filteredPatients)
                .where(0)
                .equalTo(0)
                .projectFirst(0,1);

        // Transaction
        // ([<patientID, [geneID0, geneID1, ...]>, ...])
        DataSet<Tuple2<String, List<Integer>>> transactions = joinedTable
            .groupBy(0)
            .reduceGroup(new TransactionsGroupReduceFunction());

        // Get the size of samples
        long cellSize = joinedTable
            .distinct(0)
            .count();

        // Threshold of number of transactions to keep
        double thres = (double) supportValue * cellSize;

        System.out.println("CellSize: " + new Long(cellSize).toString());
        System.out.println("Threshold: " + new Double(thres).toString());

        // Distinct gene IDs from all samples
        DataSet<ItemSet> initialGenes = joinedTable
            .distinct(1)
            .map(s -> new ItemSet(s.f1));

        // Initial number of transactions for each gene above the support
        DataSet<ItemSet> countedGenes = initialGenes
            .map(new CountGeneRichMapFunction())
            .withBroadcastSet(transactions, "transactions");
        countedGenes = countedGenes.filter(new CountGeneFilterFunction(thres));

        // Itemset with length 2, as input for iteration
        DataSet<ItemSet> iterStart = countedGenes
            .flatMap(new AddGeneRichFlatMapFunction())
            .withBroadcastSet(initialGenes, "initialGenes");

        iterStart = iterStart
            .distinct(new GeneKeySelector());

        // Begin the iteration
        IterativeDataSet<ItemSet> initial = iterStart.iterate(maxItemSize - 1);

        // Count newly added genes
        DataSet<ItemSet> iterCount = initial
            .map(new CountGeneRichMapFunction())
            .withBroadcastSet(transactions, "transactions")
            .filter(new CountGeneFilterFunction(thres));

        // Add every gene in initial list to existing itemsets 
        DataSet<ItemSet> iterAdd = iterCount
            .flatMap(new AddGeneRichFlatMapFunction())
            .withBroadcastSet(initialGenes, "initialGenes");

        // Remove duplicate from the list of itemsets
        DataSet<ItemSet> duplicateRemoved = iterAdd
            .distinct(new GeneKeySelector());

        // Terminate the iteration when nothing can be added
        // DataSet<ItemSet> output = initial.closeWith(duplicateRemoved, diff);
        DataSet<ItemSet> output = initial.closeWith(iterAdd, iterAdd);

        List<ItemSet> itemsets;
        // End the program by writing the output!
        if (params.has("output")) {
            output.first(1).print();
            itemsets = env.getLastJobExecutionResult().getAccumulatorResult("itemsets");
            Collections.sort(itemsets, new Comparator<ItemSet>() {
                public int compare(ItemSet arg0, ItemSet arg1) {
                    return arg1.getNumOfTransactions() - arg0.getNumOfTransactions();
                }
            });
            DataSet<ItemSet> resultToWrite = env.fromCollection(itemsets);
            // resultToWrite.writeAsText(params.get("output"));
            //output.writeAsText(params.get("output"));
            resultToWrite.writeAsFormattedText(params.get("output"), new ResultTextFormatter());
            env.execute();


        } else {
            // Always limit direct printing
            // as it requires pooling all resources into the driver.
            output.first(100).print();
            System.err.println("No output location specified; printing first 100.");
            itemsets = env.getLastJobExecutionResult().getAccumulatorResult("itemsets");
            Collections.sort(itemsets, new Comparator<ItemSet>() {
                public int compare(ItemSet arg0, ItemSet arg1) {
                    return arg1.getNumOfTransactions() - arg0.getNumOfTransactions();
                }
            });
            for (ItemSet is: itemsets) {
                System.out.println(is);
            }
        }
	}

    // Filter the Gene from the raw file
    // Rows with inactive gene will be filtered out
    public static class RawGeneFilterFunction implements FilterFunction<String> {
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
    }

    // Map the patientID and disease out from each patient affected by cancer
    public static class PatientsFlatMapFunction implements FlatMapFunction<String, Tuple2<String, String>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {
            String[] values = value.split(",");
            String patientID = values[0];
            String[] diseases = values[4].split(" ");
            for (String disease: diseases) {
                if (disease.equals("breast-cancer") || disease.equals("prostate-cancer") || 
                    disease.equals("pancreatic-cancer") || disease.equals("leukemia") ||
                    disease.equals("lymphoma")) {
                    out.collect(new Tuple2<String, String>(patientID, disease));    
                }
            }
        }
    }

    // Reduce The transaction from [(patientID, geneID)] data
    // Output: [(transactionID, [geneID0, geneID1, ...]), ...]
    public static class TransactionsGroupReduceFunction implements GroupReduceFunction<Tuple2<String, String>,Tuple2<String, List<Integer>>> {
        @Override
        public void reduce(Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, List<Integer>>> out) {
            List<Integer> tempList = new LinkedList<Integer>();
            String tID = "";
            for (Tuple2<String, String> tuple: values) {
                tempList.add(Integer.parseInt(tuple.f1));
                tID = tuple.f0;
            }
            out.collect(new Tuple2<String, List<Integer>>(tID, tempList));
        } 
    }

    // Format the result to save to a file
    public static class ResultTextFormatter implements TextFormatter<ItemSet> {
        @Override
        public String format(ItemSet value) {
            String output = new Integer(value.getNumOfTransactions()).toString();
            for (Integer st: value.getGeneIDs()) {
                output += ("\t" + st.toString());
            }
            return output;
        }
    }

    // Generate the key for ItemSet
    public static class GeneKeySelector implements KeySelector<ItemSet, String> {
        @Override
        public String getKey(ItemSet a) throws Exception {
            String key = "";
            LinkedList<Integer> tmp = a.getGeneIDs();
            for (Integer st: tmp) {
                key += st;
            }
            return key;
        }
    }


    public static class AddGeneRichFlatMapFunction extends RichFlatMapFunction<ItemSet, ItemSet> {
        private List<ItemSet> initialGenes;
        private ListAccumulator<ItemSet> itemsets;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.initialGenes = getRuntimeContext().
            getBroadcastVariable ("initialGenes");
            this.itemsets = new ListAccumulator<ItemSet>();
            getRuntimeContext().addAccumulator("itemsets", this.itemsets);
        }

        @Override
        public void flatMap(ItemSet input, Collector<ItemSet> out) {
            this.itemsets.add(input);
            for (ItemSet gene: initialGenes) {
                LinkedList<Integer> inputGene = input.getGeneIDs();
                LinkedList<Integer> output = new LinkedList<Integer>(inputGene);
                if (!output.containsAll(gene.getGeneIDs())) {
                    output.addAll(gene.getGeneIDs());
                    Collections.sort(output);
                    out.collect(new ItemSet(output));
                }
            }
        }
    }

    // Function used to count the number of genes
    public static class CountGeneFilterFunction implements FilterFunction<ItemSet> {
        final private double thres;

        public CountGeneFilterFunction(double thres) {
            this.thres = thres;
        }
        @Override
        public boolean filter(ItemSet value) {
            return value.filter((int) this.thres);
        }
    }

    // The function is used to count each itemset with
    // the transaction broadcast variable
    public static class CountGeneRichMapFunction extends RichMapFunction<ItemSet, ItemSet> {
        private List<Tuple2<String, List<Integer>>> transactions;
        @Override
        public void open(Configuration parameters) throws Exception {
            this.transactions = getRuntimeContext().
            getBroadcastVariable ("transactions");
        }

        @Override
        public ItemSet map(ItemSet value) {
            return countItem(value, transactions);
        }
    }

    // Count the number of genes with arbitrary size in each transaction
    // Input format:
    // genes [geneID1, geneID2, ...], trans [[geneID1, ...], ...]
    // Output format:
    // <[geneID1, geneID2, ...], count>
    public static ItemSet countItem(ItemSet genes, List<Tuple2<String, List<Integer>>> trans) {
        int count = 0;
        for (Tuple2<String, List<Integer>> tran: trans) {
            if (tran.f1.containsAll(genes.getGeneIDs())) {
                count += 1;
            }
        }
        genes.setNumOfTransactions(count);
        return genes;
    }

}



