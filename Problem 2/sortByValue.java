package yelp;

import java.util.Comparator;
import java.util.Map;

public class sortByValue implements Comparator<Object> {
 
	Map<String, Double> temp_map;
 
	public sortByValue(Map<String, Double> map) {
		this.temp_map = map;
	}
	public int compare(Object value1, Object value2) {
		
		Double valueA= (Double) temp_map.get(value1);
		Double valueB= (Double) temp_map.get(value2);
		int compare=valueB.compareTo(valueA);
		if(compare==0)
			return 1;		
		return compare;
	}
}
