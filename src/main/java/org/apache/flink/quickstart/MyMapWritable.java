package org.apache.flink.quickstart;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class MyMapWritable extends MapWritable{
	

	@Override
	public String toString(){
		String result = new String();
		for (Entry<Writable, Writable> entry: this.entrySet()){
			result += entry.getKey().toString();
			result += ":";
			result += entry.getValue().toString();
			result += "|";
		}
		
		return result;
	}
}
