package ml.apriori;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class TransactionGroupReduceFunction implements GroupReduceFunction<Tuple2<String, String>, Tuple2<String, ArrayList<Integer>>> {

	private static final long serialVersionUID = 1L;

	@Override
	public void reduce(Iterable<Tuple2<String, String>> arg0, Collector<Tuple2<String, ArrayList<Integer>>> arg1)
			throws Exception {
		ArrayList<Integer> items = new ArrayList<>();
		String tid = null;
		for (Tuple2<String, String> transaction : arg0) {
			items.add(Integer.parseInt(transaction.f1));
			tid = transaction.f0;
		}
		arg1.collect(new Tuple2<String, ArrayList<Integer>>(tid, items));
	}

}
