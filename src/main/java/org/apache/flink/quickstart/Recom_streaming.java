package org.apache.flink.quickstart;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Recom_streaming {
	@SuppressWarnings("serial")
	public static final class MyFlatMap implements FlatMapFunction<String, Tuple2<String, MyMapWritable>> {
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
	
	/*public static final class ProductMapReduce implements WindowMapFunction<Tuple2<String, MyMapWritable>,Tuple2<String,MyMapWritable>>{

		@Override
		public void reduceWindow(Iterable<Tuple2<String, MyMapWritable>> in,
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
	}*/
	public static final class StreamProductMapReduce implements ReduceFunction<Tuple2<String, MyMapWritable>>{
		@Override
		public Tuple2<String,MyMapWritable> reduce(Tuple2<String,MyMapWritable> arg0,Tuple2<String,MyMapWritable> arg1)
				throws Exception {
			MyMapWritable result = new MyMapWritable();
			for(Entry<Writable, Writable> entry : arg0.f1.entrySet()) {	
				Text myMapKey = new Text();
				myMapKey.set(entry.getKey().toString());
				IntWritable myMapValue = new IntWritable(Integer.parseInt(entry.getValue().toString()));
				if(result.containsKey(myMapKey)){
					int sum = Integer.parseInt(result.get(myMapKey).toString());
					sum = sum + Integer.parseInt(myMapValue.toString());
					result.put(myMapKey, new IntWritable(sum));
				} else {
					result.put(myMapKey, new IntWritable(1));
				}

			}
			for(Entry<Writable, Writable> entry : arg1.f1.entrySet()) {	
				Text myMapKey = new Text();
				myMapKey.set(entry.getKey().toString());
				IntWritable myMapValue = new IntWritable(Integer.parseInt(entry.getValue().toString()));
				if(result.containsKey(myMapKey)){
					int sum = Integer.parseInt(result.get(myMapKey).toString());
					sum = sum + Integer.parseInt(myMapValue.toString());
					result.put(myMapKey, new IntWritable(sum));
				} else {
					result.put(myMapKey, new IntWritable(1));
				}
			}
			
			// TODO Auto-generated method stub

			Tuple2<String, MyMapWritable> ret = new Tuple2<String, MyMapWritable>(arg0.f0.toString(), result);
			return ret;
		}
	}
	public static void main(String [] args) throws Exception{
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStream<Tuple2<String, MyMapWritable>> datastream = env
				.socketTextStream("localhost", 9999)
				.flatMap(new MyFlatMap())
				.groupBy(0)
				.reduce(new StreamProductMapReduce());

		datastream.print();
		env.execute("Recommendation");
		//DataSet<String> text = env.readTextFile(Path);
	} 
}
