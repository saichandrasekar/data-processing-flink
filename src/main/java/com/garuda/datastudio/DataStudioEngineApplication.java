package com.garuda.datastudio;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.garuda.datastudio.models.Person;

public class DataStudioEngineApplication {

	public static void main(String a[]) {
		System.out.println("Data Studio - Engine Application...is alive!!");

		final StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment
				.getExecutionEnvironment();

		DataStream<Person> personDataStream = streamExecutionEnvironment.fromElements(new Person("Fred", 35),
				new Person("Wilma", 24), new Person("Simba", 12));

		DataStream<Person> adultsDataStream = personDataStream.filter(new FilterFunction<Person>() {

			@Override
			public boolean filter(Person person) throws Exception {
				return person.getAge() > 12;
			}
		});

		adultsDataStream.print();

		try {
			streamExecutionEnvironment.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
