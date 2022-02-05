/**
 * 
 */
package com.garuda.datastudio;

import java.io.File;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author saich
 *
 */
public class HotelGroceryProcessorApplication {

	private static Logger logger = LoggerFactory.getLogger(HotelGroceryProcessorApplication.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		logger.info("Entering HotelGroceryProcessorApplication :: main");

		final StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment
				.getExecutionEnvironment();

		File hotelDataFile = new File("S:/02-Learning/01-Apache_Flink/02-test_data/hotel_grocery.csv");
		logger.debug("Is the input file present: " + hotelDataFile.exists());

		DataStream<String> hotelDataStream = streamExecutionEnvironment
				.readTextFile("S:/02-Learning/01-Apache_Flink/02-test_data/hotel_grocery.csv");

		DataStream<String> juiceDataStream = hotelDataStream.filter(new FilterFunction<String>() {

			@Override
			public boolean filter(String value) throws Exception {
				String[] hotelData = value.split(",");

				String dish = hotelData[4];
				
				double rating = 0;
				int availability = 0;
				try {
					rating = Double.parseDouble(hotelData[3]);
					availability = Integer.parseInt(hotelData[hotelData.length-1]);
				}catch(NumberFormatException formatException) {
					System.out.println("Wrong Rating value: "+hotelData[3]+" for id: "+value);
				}
				
				
				
				if (rating > 4.0 && dish.toUpperCase().contains("A") && availability<10) {
					return true;
				}
				return false;
			}
		});

		juiceDataStream.print();

		try {
			streamExecutionEnvironment.execute();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}

		logger.info("Exiting HotelGroceryProcessorApplication :: main");
	}

}
