package org.apache.flink.quickstart;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class Recommendation {
	@SuppressWarnings("serial")
	public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, MyMapWritable>> {
		public void flatMap(String value, Collector<Tuple2<String, MyMapWritable>> out) {
			MyMapWritable productMap = new MyMapWritable();
			ArrayList<String> products = new ArrayList<>();
			String[] tokens = value.toLowerCase().split(" ");
			IntWritable one = new IntWritable(1);
			for (String token : tokens) {
				if (!products.contains(token) ){
					products.add(token);
					productMap.put(new Text(token), one);
				}
			}
			for (String product : products){
				//System.out.println("before: "+ productMap.keySet().toString());
				//System.out.println(product);
				productMap.remove(new Text(product));
				//System.out.println("after: "+productMap.keySet().toString());
				out.collect(new Tuple2<String, MyMapWritable>(product,productMap));
				
				productMap.put(new Text(product),one);
			}
		}
	}
	@SuppressWarnings("serial")
	public static final class ProductMapReduce implements GroupReduceFunction<Tuple2<String, MyMapWritable>,Tuple2<String,MyMapWritable>>{

		@Override
		public void reduce(Iterable<Tuple2<String, MyMapWritable>> in,
				Collector<Tuple2<String, MyMapWritable>> out) throws Exception {
			// TODO Auto-generated method stub
			Map<String,MyMapWritable> AllProductMap = new HashMap<String,MyMapWritable>();
			MyMapWritable ProductMap = new MyMapWritable();
			//System.out.println(in.toString());
			for (Tuple2<String,MyMapWritable> UserProductMap : in){
				//System.out.println("One Map: "+ UserProductMap.f0+"  "+UserProductMap.f1);
				//MyMapWritable ProductMap = new MyMapWritable();
				String MapKey = UserProductMap.f0;
				MyMapWritable MapValue = UserProductMap.f1;
				for(Entry<Writable, Writable> entry : MapValue.entrySet()){
					Text entryKey = new Text(entry.getKey().toString());
					IntWritable entryValue = new IntWritable(Integer.parseInt(entry.getValue().toString()));
					if (ProductMap.containsKey(entryKey)){
						int count = Integer.parseInt(entryValue.toString()) + Integer.parseInt(ProductMap.get(entryKey).toString());
						ProductMap.put(entryKey, new IntWritable(count));
					} else{
						ProductMap.put(entryKey, new IntWritable(1));
						//ProductMap.put(entryKey, entryValue);
					}
				}
				//AllProductMap.put(new Text(MapKey), ProductMap);
				//System.out.println("out: "+MapKey+"  " + ProductMap.toString());
				AllProductMap.put(MapKey,ProductMap);
				//out.collect(new Tuple2<String,MyMapWritable>(MapKey,ProductMap));
			}
			for (String ProductKey : AllProductMap.keySet()){
				out.collect(new Tuple2<String,MyMapWritable>(ProductKey.toString(),AllProductMap.get(ProductKey)));
			}
			//out.collect(new Tuple2<String,MyMapWritable>(MapKey,ProductMap));
			
			
		}
	}
	public static void main(String [] args) throws Exception{
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<String> input = env.fromElements(
				"book1 dvd1 cd2 book2",
				"dvd2 cd2 book2 cd1",
				"book1 dvd2 book2 cd1",
				"dvd1 book2 cd1 dvd2"
				);

		DataSet<Tuple2<String,MyMapWritable>> output = input.flatMap(new LineSplitter())
														.groupBy(0)
														.reduceGroup(new ProductMapReduce());
		output.print();
		
	} 
}
