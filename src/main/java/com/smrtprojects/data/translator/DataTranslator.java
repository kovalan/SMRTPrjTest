package com.smrtprojects.data.translator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class DataTranslator {

	private static final String TAB_CHAR = "\t";

	public static void main(String[] args) {

		if (args.length > 3 && args[0] != null && args[1] != null
				&& args[2] != null && args[3] != null) {
			
			boolean isSuccess = translateData(args[0], args[1], args[2], args[3]);
			
			if(isSuccess) {
				System.out.println("Translated successfully and the translated file is available at: "+ args[3]);
				return;
			}
			
		}else {
			System.out.println("Please provide valid file paths as program arguments");
		}
	}

	/**
	 * Translate the given data file based on the configuration.
	 * 
	 * @param dataFilePath
	 * @param columnConfigPath
	 * @param vendorIdConfig
	 * @param outputFilePath
	 * @return boolean - translation success or failure.
	 */
	public static boolean translateData(String dataFilePath,
			String columnConfigPath, String vendorIdConfig,
			String outputFilePath) {

		String inputRowData = null;
		BufferedReader buffInputFileReader = null;
		BufferedWriter buffOutputFileWriter = null;

		try {
			// Retrieve column and vendor Id configurations.
			Map<String, String> columnConfigMap = readConfig(columnConfigPath);
			Map<String, String> vendorIdConfigMap = readConfig(vendorIdConfig);

			if (columnConfigMap != null && !columnConfigMap.isEmpty()
					&& vendorIdConfigMap != null
					&& !vendorIdConfigMap.isEmpty()) {

				buffInputFileReader = new BufferedReader(new InputStreamReader(
						new FileInputStream(dataFilePath)));

				// Read the column names in the first row
				inputRowData = buffInputFileReader.readLine();

				// Proceed only if column names are found in the first row.
				if (inputRowData != null && !inputRowData.isEmpty()) {

					// Proceed only if output file is found.
					if (createFileIfNotExists(outputFilePath)) {
						
						buffOutputFileWriter = new BufferedWriter(
								new FileWriter(outputFilePath));

						// Write output column names and return reqColumnNos
						// that has the index of the columns to be extracted.
						Set<Integer> reqColumnNos = writeOutputColumnNames(
								inputRowData, columnConfigMap,
								buffOutputFileWriter);

						// Proceed only if the required columns exist in the data file.
						if (!reqColumnNos.isEmpty()) {

							// Read the row data line by line and write to the output file.
							while ((inputRowData = buffInputFileReader.readLine()) != null) {
								
								writeOutputRowData(inputRowData,
										vendorIdConfigMap, reqColumnNos,
										buffOutputFileWriter);
							}
						}
						
						return true;//Success message
					}
				}
			}
		} catch (IOException e) {
			// TODO log the exception instead.
			e.printStackTrace();
		} finally {
			close(buffOutputFileWriter, buffInputFileReader);
		}
		
		return false;

	}

	/**
	 * Reads the current data line and extracts the column names
	 * 
	 * @param headerRowData
	 *            - data line
	 * @param columnConfigMap
	 *            - map containing columns to be extracted and its new name.
	 * @param buffOutputFileWriter
	 *            - output file writer
	 * @return reqColumnNos - index of columns that are required.
	 */
	private static Set<Integer> writeOutputColumnNames(String headerRowData,
			Map<String, String> columnConfigMap,
			BufferedWriter buffOutputFileWriter) throws IOException {

		StringBuilder outputBuilder = new StringBuilder();

		String[] columnNames = splitByTab(headerRowData);

		Set<Integer> reqColumnNos = null;

		if (columnNames != null && columnNames.length != 0) {

			Integer columnNum = 0;

			reqColumnNos = new TreeSet<Integer>();

			for (String clmName : columnNames) {

				// Get the custom column name if the column is present in the
				// configuration
				if (columnConfigMap.containsKey(clmName)) {

					// Get "OUR" new column name.
					outputBuilder.append(columnConfigMap.get(clmName));
					outputBuilder.append(TAB_CHAR);
					reqColumnNos.add(columnNum++);
				} else {
					columnNum++;
				}

			}

			if (!reqColumnNos.isEmpty()) {
				// Remove trailing tab space
				removeTrailingChar(outputBuilder);
				// Go to next line
				addNewLineChar(outputBuilder);
				
				buffOutputFileWriter.write(outputBuilder.toString());
			}

			
		}

		return reqColumnNos;
	}

	/**
	 * Reads the current data line and extracts the data of the listed vendor id
	 * 
	 * @param rowData
	 *            - data line
	 * @param vendorIdConfigMap
	 *            - map containing vendor id to be extracted and its new name.
	 * @param reqColumnNos
	 *            - index of columns that are required.
	 * @param buffOutputFileWriter
	 *            - output file writer
	 */
	private static void writeOutputRowData(String rowData,
			Map<String, String> vendorIdConfigMap, Set<Integer> reqColumnNos,
			BufferedWriter buffOutputFileWriter) throws IOException {

		StringBuilder outputBuilder = new StringBuilder();

		String[] rowDataArray = splitByTab(rowData);
		if (rowDataArray != null && rowDataArray.length > 0) {

			String vendorId = rowDataArray[0];

			// Append the row only if the vendor id is listed in configuration
			// file.
			if (vendorIdConfigMap.containsKey(vendorId)) {

				for (Integer reqClmNo : reqColumnNos) {

					if (reqClmNo == 0) {
						// Update with the new vendor Id.
						outputBuilder.append(vendorIdConfigMap.get(vendorId));
					} else {
						outputBuilder.append(rowDataArray[reqClmNo]);
					}

					outputBuilder.append(TAB_CHAR);
				}

				// Remove trailing tab space
				removeTrailingChar(outputBuilder);
				// Go to next line
				addNewLineChar(outputBuilder);

				buffOutputFileWriter.write(outputBuilder.toString());
			}
		}

	}

	/**
	 * Read configuration file from the path and create a key value pair.
	 * 
	 * @param columnConfigPath
	 * @return key value map
	 */
	private static Map<String, String> readConfig(String configPath)
			throws IOException {

		Map<String, String> columnConfigMap = new HashMap<String, String>();
		String configData = null;
		BufferedReader buffLineReader = null;

		try {
			buffLineReader = new BufferedReader(new InputStreamReader(
					new FileInputStream(configPath)));

			// Read the configuration data line by line.
			while ((configData = buffLineReader.readLine()) != null) {

				String[] configDataArray = splitByTab(configData);
				if (configDataArray != null && configDataArray.length == 2) {
					columnConfigMap.put(configDataArray[0], configDataArray[1]);
				}
			}

		} catch (IOException e) {

			throw e;

		} finally {

			try {
				if (buffLineReader != null) {
					buffLineReader.close();
				}
			} catch (IOException e) {

				throw e;
			}
		}

		return columnConfigMap;
	}

	private static boolean createFileIfNotExists(String fileName) {
		File file = new File(fileName);
		boolean b = true;
		if (!file.exists()) {
			try {
				if (file.getParentFile().exists()
						|| file.getParentFile().mkdirs()) {
					b = file.createNewFile();
				} else {
					return false;
				}
			} catch (IOException e) {
				// TODO add logger instead.
				e.printStackTrace();
				return false;
			}
		}
		return b;
	}

	private static void close(BufferedWriter writer, BufferedReader reader) {
		try {
			if (writer != null) {
				writer.close();
			}
			if(reader != null) {
				reader.close();
			}
		} catch (IOException e) {
			// TODO add logger instead.
			e.printStackTrace();
		}
	}

	/**
	 * Split the given string with tab char
	 * 
	 * @param givenString
	 * @return
	 */
	private static String[] splitByTab(String givenString) {

		String[] strArray = null;

		if (givenString != null && !givenString.isEmpty()) {
			strArray = givenString.split(TAB_CHAR);
		}
		return strArray;
	}

	/**
	 * Remove last character in a string builder
	 * 
	 * @param stringBuilder
	 */
	private static void removeTrailingChar(StringBuilder stringBuilder) {
		if (stringBuilder != null && stringBuilder.length() > 0) {
			stringBuilder.setLength(stringBuilder.length() - 1);
		}
	}

	/**
	 * Add next line char to the end of String builder
	 * 
	 * @param stringBuilder
	 */
	private static void addNewLineChar(StringBuilder stringBuilder) {
		if (stringBuilder != null && stringBuilder.length() > 0) {
			stringBuilder.append(System.getProperty("line.separator"));
		}
	}
}
