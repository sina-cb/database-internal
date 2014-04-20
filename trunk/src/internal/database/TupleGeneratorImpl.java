package internal.database;

/*******************************************************************************
 * @file  TupleGeneratorImpl
 *
 * @author   Sadiq Charaniya, John Miller
 */

import static java.lang.System.out;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/*******************************************************************************
 * This class is used to populate a database (collection of tables) with
 * randomly generated values that satisfy the following integrity constraints:
 * domain, primary keys and foreign key constraints.
 */
@SuppressWarnings("all")
public class TupleGeneratorImpl implements TupleGenerator {
	/**
	 * Counter for table numbers
	 */
	private int counter = 0;

	/**
	 * Initializations
	 */
	private HashMap<String, Comparable[][]> result = new HashMap<>();

	private HashMap<Integer, String> tableIndex = new HashMap<>();

	private HashMap<String, String[]> tableAttr = new HashMap<>();

	private HashMap<String, String[]> tableDomain = new HashMap<>();

	private HashMap<String, String[]> tablepks = new HashMap<>();

	HashMap<String, String[][]> tablefks = new HashMap<>();

	/***************************************************************************
	 * Adding relation to Schema.
	 * 
	 * @param name
	 *            the name of the table
	 * @param attribute
	 *            the array of attributes
	 * @param domain
	 *            the array of domains
	 * @param primaryKey
	 *            the array of primary keys
	 * @param foriegnKey
	 *            the array of foriegn keys
	 */
	public void addRelSchema(String name, String[] attribute, String[] domain,
			String[] primaryKey, String[][] foreignKey) {
		tableIndex.put(counter, name);
		tableAttr.put(name, attribute);
		tableDomain.put(name, domain);
		tablepks.put(name, primaryKey);
		tablefks.put(name, foreignKey);
		counter++;
	} // addRelSchema

	/***************************************************************************
	 * Adding relation to Schema. Convenience method.
	 * 
	 * @param name
	 *            the name of the table
	 * @param attribute
	 *            the string embedding the table's attributes
	 * @param domain
	 *            the string embedding the table's domains
	 * @param primaryKey
	 *            the string embedding the table's primary keys
	 * @param foriegnKey
	 *            the array of foriegn keys
	 */
	public void addRelSchema(String name, String attribute, String domain,
			String primaryKey, String[][] foreignKey) {
		addRelSchema(name, attribute.split(" "), domain.split(" "),
				primaryKey.split(" "), foreignKey);
	} // addRelSchema

	/***************************************************************************
	 * Generates random tuples that satisfy all the integrity constraints.
	 * 
	 * @param tuples
	 *            the number of tuples for each table
	 * @return tempResult contains tuples for all the tables in the order they
	 *         were added
	 */
	public Comparable[][][] generate(int[] tuples) {
		Random rand = new Random();
		String tableName = "";
		String[] attribute;
		String[] domain;
		String[] pks;
		String[][] fks;
		Set<String> pKeys = new HashSet<String>();
		Set<Comparable<?>> pKeyValues = new HashSet<Comparable<?>>();
		Set<String> fKeys = new HashSet<String>();
		List<String> fkIndex = new ArrayList<String>();
		int iVal;
		String sVal;
		double dVal;

		for (int i = 0; i < tuples.length; i++) {
			tableName = tableIndex.get(i);
			attribute = tableAttr.get(tableName);
			domain = tableDomain.get(tableName);
			pks = tablepks.get(tableName);
			fks = tablefks.get(tableName);
			Comparable[][] subResult = new Comparable[tuples[i]][attribute.length];

			// out.println (tableName);
			for (int n = 0; n < pks.length; n++)
				pKeys.add(pks[n]);

			// ----------------------------------------------------
			// Handle the case where the table has no foreign keys

			if (fks == null) {
				for (int j = 0; j < tuples[i]; j++) {
					for (int k = 0; k < attribute.length; k++) {
						if (pKeys.contains(attribute[k])) { // key requires
															// uniqueness

							switch (domain[k]) {
							case "Integer":
								for (iVal = rand.nextInt(max); pKeyValues
										.contains(iVal); iVal = rand
										.nextInt(max))
									;
								subResult[j][k] = iVal;
								pKeyValues.add(iVal);
								break;
							case "String":
								switch (attribute[k]) {
								case "ipAddress":
									for (sVal = generateIP(); pKeyValues.contains(sVal); sVal = generateIP());
									subResult[j][k] = sVal;
									pKeyValues.add(sVal);
									break;
								case "PCatName":
									sVal = generatePCatName();
									subResult[j][k] = sVal;
									pKeyValues.add(sVal);
									break;
								case "SCatName":
									sVal = generateSCatName();
									subResult[j][k] = sVal;
									pKeyValues.add(sVal);
									break;
								case "ShipCatName":
									sVal = generateShipCatName();
									subResult[j][k] = sVal;
									pKeyValues.add(sVal);
									break;
								default:
									for (sVal = attribute[k]
											+ rand.nextInt(max); pKeyValues
											.contains(sVal); sVal = attribute[k]
											+ rand.nextInt(max))
										;
									subResult[j][k] = sVal;
									pKeyValues.add(sVal);
									break;
								}
								break;
							case "Double":
								for (dVal = rand.nextInt(max)
										* rand.nextDouble(); pKeyValues
										.contains(dVal); dVal = rand
										.nextInt(max) * rand.nextDouble())
									;
								subResult[j][k] = dVal;
								pKeyValues.add(dVal);
								break;
							default:
								throw new IllegalArgumentException(
										"Invalid type in switch: " + domain[k]);
							} // switch

						} else { // non-key does not require uniqueness

							switch (domain[k]) {
							case "Integer":
								subResult[j][k] = rand.nextInt(max);
								break;
							case "String":
								subResult[j][k] = attribute[k]
										+ rand.nextInt(max);
								break;
							case "Double":
								subResult[j][k] = rand.nextInt(100000)
										* rand.nextDouble();
								break;
							default:
								throw new IllegalArgumentException(
										"Invalid type in switch: " + domain[k]);
							} // switch

						} // if
					} // for
				} // for

				// -------------------------------------------------
				// Handle the case where the table has foreign keys (maintain
				// referential integrity)

			} else {
				for (int j = 0; j < tuples[i]; j++) {
					for (int n = 0; n < fks.length; n++) {

						if (!fks[n][0].contains(" ")) {
							fkIndex.add(n, fks[n][0]);
							Comparable[][] fkTable = result.get(fks[n][1]);
							int s;
							for (s = 0; s < attribute.length; s++) {
								if (attribute[s].equals(fks[n][0]))
									break;
							} // for
							String[] tempAtr = tableAttr.get(fks[n][1]);
							int x;
							for (x = 0; x < tempAtr.length; x++) {
								if (tempAtr[x].equals(fks[n][2]))
									break;
							} // for
							subResult[j][s] = fkTable[rand
									.nextInt(fkTable.length)][x];

						} else {
							String[] sfks = fks[n][0].split(" ");
							String[] rfks = fks[n][2].split(" ");
							for (int z = 0; z < fks[n][0].split(" ").length; z++) {
								fkIndex.add(n + z, fks[n][0].split(" ")[z]);
							} // for
							Comparable[][] fkTable = result.get(fks[n][1]);
							if (fkTable == null) {
								out.println("Foreign Key Error: "
										+ "table containing referencing key cannot be populated before referenced table");
								out.println("Possible Solution: Add '"
										+ fks[n][1] + "' table before adding '"
										+ tableName + "' table.");
								System.exit(0);
							} // if
							int t = rand.nextInt(fkTable.length);
							for (int a = 0; a < sfks.length; a++) {
								int b;
								for (b = 0; b < attribute.length; b++) {
									if (attribute[b].equals(sfks[a]))
										break;
								} // for
								String[] tempAtr = tableAttr.get(fks[n][1]);
								int c;
								for (c = 0; c < tempAtr.length; c++) {
									if (tempAtr[c].equals(rfks[a]))
										break;
								} // for
								subResult[j][b] = fkTable[t][c];
							} // for
						} // if
					} // for

					for (int k = 0; k < attribute.length; k++) {
						if (!fkIndex.contains(attribute[k])) {
							if (pKeys.contains(attribute[k])) {

								switch (domain[k]) {
								case "Integer":
									for (iVal = rand.nextInt(max); pKeyValues
											.contains(iVal); iVal = rand
											.nextInt(max))
										;
									subResult[j][k] = iVal;
									pKeyValues.add(iVal);
									break;
								case "String":
									switch (attribute[k]) {
									case "ipAddress":
										for (sVal = generateIP(); pKeyValues.contains(sVal); sVal = generateIP());
										subResult[j][k] = sVal;
										pKeyValues.add(sVal);
										break;
									case "PCatName":
										sVal = generatePCatName();
										subResult[j][k] = sVal;
										pKeyValues.add(sVal);
										break;
									case "SCatName":
										sVal = generateSCatName();
										subResult[j][k] = sVal;
										pKeyValues.add(sVal);
										break;
									case "ShipCatName":
										sVal = generateShipCatName();
										subResult[j][k] = sVal;
										pKeyValues.add(sVal);
										break;
									default:
										for (sVal = attribute[k]
												+ rand.nextInt(max); pKeyValues
												.contains(sVal); sVal = attribute[k]
												+ rand.nextInt(max))
											;
										subResult[j][k] = sVal;
										pKeyValues.add(sVal);
										break;
									}
									break;
								case "Double":
									for (dVal = rand.nextInt(max)
											* rand.nextDouble(); pKeyValues
											.contains(dVal); dVal = rand
											.nextInt(max)
											* rand.nextDouble())
										;
									subResult[j][k] = dVal;
									pKeyValues.add(dVal);
									break;
								default:
									throw new IllegalArgumentException(
											"Invalid type in switch: "
													+ domain[k]);
								} // if

							} else {

								switch (domain[k]) {
								case "Integer":
									subResult[j][k] = rand.nextInt(max);
									break;
								case "String":
									subResult[j][k] = attribute[k]
											+ rand.nextInt(max);
									break;
								case "Double":
									subResult[j][k] = rand.nextInt(max)
											* rand.nextDouble();
									break;
								default:
									throw new IllegalArgumentException(
											"Invalid type in switch: "
													+ domain[k]);
								} // if

							} // if
						} // if
					} // for
				} // for
			} // if

			pKeys.clear();
			fkIndex.clear();
			result.put(tableName, subResult);
		} // for

		Comparable[][][] tempResult = new Comparable[result.size()][][];

		for (int i = 0; i < result.size(); i++) {
			tableName = tableIndex.get(i);
			Comparable[][] subTable = result.get(tableName);
			tempResult[i] = subTable;
		} // for

		return tempResult;
	} // generate

	private String generateSCatName() {
		BufferedReader br;
		String result = "";
		try {
			br = new BufferedReader(new FileReader(new File("StoreCatNames.txt")));
			for (int i = 0; i <= SCatNameIndex; i++) {
				result = br.readLine();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		SCatNameIndex++;
		return result;
	}
	
	private String generateShipCatName() {
		BufferedReader br;
		String result = "";
		try {
			br = new BufferedReader(new FileReader(new File("ShipCatNames.txt")));
			for (int i = 0; i <= ShipCatNameIndex; i++) {
				result = br.readLine();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		ShipCatNameIndex++;
		return result;
	}

	private String generatePCatName() {
		BufferedReader br;
		String result = "";
		try {
			br = new BufferedReader(new FileReader(new File("ProductCatNames.txt")));
			for (int i = 0; i <= PCatNameIndex; i++) {
				result = br.readLine();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		PCatNameIndex++;
		return result;
	}

	private static String generateIP() {
		Random rand = new Random();
		int num1 = rand.nextInt(256);
		int num2 = rand.nextInt(256);
		int num3 = rand.nextInt(256);
		int num4 = rand.nextInt(256);
		
		return (num1 + "." + num2 + "." + num3 + "." + num4);
	}
	
	int max = Integer.MAX_VALUE;
	
	static int PCatNameIndex = 0;
	static int SCatNameIndex = 0;
	static int ShipCatNameIndex = 0;
	
} // TestGeneratorImpl class
