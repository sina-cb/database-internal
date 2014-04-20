package internal.database;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.commons.math3.util.Pair;

public class MyTupleGenerator {

	@SuppressWarnings("rawtypes")
	public static void main(String[] args) throws Exception {

		TupleGenerator test = new TupleGeneratorImpl();
		
		test.addRelSchema("PRODUCT_CAT", "PCatName",
				"String", "PCatName", null);

		test.addRelSchema("PRODUCT", "ProdId ProdName ProdCategory",
				"String String String", "ProdId", new String[][] {
				{ "ProdCategory", "PRODUCT_CAT", "PCatName" }});
		
		test.addRelSchema("STORE_CAT", "SCatName",
				"String", "SCatName", null);
		
		test.addRelSchema("STORE", "StoreId StoreAdd StoreZip StoreCategory",
				"String String String String", "StoreId", new String[][] {
				{ "StoreCategory", "STORE_CAT", "SCatName" }});
		
		test.addRelSchema("CUSTOMER", "CustId CustName sex age CustAddress CustZip",
				"String String String Integer String String", "CustId", null);
		
		test.addRelSchema("PRICING", "ProdId StoreId Price Stock",
				"String String Integer Integer", "ProdId StoreId", new String[][] {
				{ "ProdId", "PRODUCT", "ProdId" },
				{ "StoreId", "STORE", "StoreId" }});
		
		test.addRelSchema("SHIPMENT_CAT", "ShipCatName",
				"String", "ShipCatName", null);
		
		test.addRelSchema("SHIPMENT", "ShipCategory ProdId StoreId ShipPrice",
				"String String String Integer", "ShipCategory ProdId StoreId", new String[][] {
				{ "ShipCategory", "SHIPMENT_CAT", "ShipCatName" },
				{ "ProdId StoreId", "PRICING", "ProdId StoreId" }});
		
		test.addRelSchema("PROMOTION", "PromoId PromoCodePhrase ShipCategory ProdId StoreId Discount StartDate EndDate",
				"String String String String String Integer String String", "PromoId", new String[][] {
				{ "ShipCategory", "SHIPMENT_CAT", "ShipCatName" },
				{ "ProdId StoreId", "PRICING", "ProdId StoreId" }});
		
		test.addRelSchema("PURCHASE", "CustId ProdId StoreId PromoId ShipCategory Feedback Payment Timestamp",
				"String String String String String Integer Integer String", "CustId ProdId StoreId", new String[][] {
				{ "PromoId", "PROMOTION", "PromoId" },
				{ "ProdId StoreId ShipCategory", "SHIPMENT", "ProdId StoreId ShipCategory" }});
		
		test.addRelSchema("B_SOCIAL", "ipAddress CustId",
				"String String", "ipAddress", new String[][] {
				{ "CustId", "CUSTOMER", "CustId" }});
		
		test.addRelSchema("B_POST", "url ipAddress TextStr Timestamp",
				"String String String String", "url", new String[][] {
				{ "ipAddress", "B_SOCIAL", "ipAddress" }});
		
		test.addRelSchema("B_COMMENT", "ipAddress PostUrl TextStr Timestamp",
				"String String String String", "ipAddress PostUrl TextStr", new String[][] {
				{ "ipAddress", "B_SOCIAL", "ipAddress" },
				{ "PostUrl", "B_POST", "url" }});
		
		test.addRelSchema("F_SOCIAL", "fId CustId",
				"String String", "fId", new String[][] {
				{ "CustId", "CUSTOMER", "CustId" }});
		
		test.addRelSchema("F_POST", "url fId TextStr Timestamp",
				"String String String String", "url", new String[][] {
				{ "fId", "F_SOCIAL", "fId" }});
		
		test.addRelSchema("F_COMMENT", "fId PostUrl TextStr Timestamp",
				"Str+ing String String String", "fId PostUrl TextStr", new String[][] {
				{ "fId", "F_SOCIAL", "fId" },
				{ "PostUrl", "F_POST", "url" }});
		
		test.addRelSchema("G_SOCIAL", "gId CustId",
				"String String", "gId", new String[][] {
				{ "CustId", "CUSTOMER", "CustId" }});
		
		test.addRelSchema("G_POST", "url gId TextStr Timestamp",
				"String String String String", "url", new String[][] {
				{ "gId", "G_SOCIAL", "gId" }});
		
		test.addRelSchema("G_COMMENT", "gId PostUrl TextStr Timestamp",
				"String String String String", "gId PostUrl TextStr", new String[][] {
				{ "gId", "G_SOCIAL", "gId" },
				{ "PostUrl", "G_POST", "url" }});
		
		test.addRelSchema("T_SOCIAL", "tId CustId",
				"String String", "tId", new String[][] {
				{ "CustId", "CUSTOMER", "CustId" }});
		
		test.addRelSchema("TWEET", "tId TextStr Timestamp",
				"String String String", "tId TextStr", null);
		
		test.addRelSchema("G_TREND", "gtId Word City Country Hits Timestamp",
				"String String String String Integer String", "gtId", null);
		
		
		
		String[] tables = { "PRODUCT_CAT", "PRODUCT", "STORE_CAT", "STORE", "CUSTOMER", "PRICING", "SHIPMENT_CAT", "SHIPMENT", "PROMOTION", "PURCHASE",
							"B_SOCIAL", "B_POST", "B_COMMENT", "F_SOCIAL", "F_POST", "F_COMMENT", "G_SOCIAL", "G_POST", "G_COMMENT", "T_SOCIAL", "TWEET", "G_TREND"};
		int tups[] = new int[] { 150 /*ProdCat*/, 100 /*Product*/, 24 /*StoreCat*/, 100 /*Store*/, 120 /*Customer*/, 100 /*Pricing*/, 12 /*ShipmentCat*/, 100 /*Shipment*/, 
				100 /*Promotion*/, 100 /*Purchase*/, 50 /*B_Social*/, 100 /*B_POST*/, 200 /*B_COMMENT*/, 50 /*F_SOCIAL*/, 100 /*F_POST*/, 100 /*F_COMMENT*/, 50 /*G_SOCIAL*/, 
				100 /*G_POST*/, 100 /*G_COMMENT*/, 50 /*T_SOCIAL*/, 300 /*TWEET*/, 300 /*G_TREND*/};
		
		Comparable[][][] resultTest = test.generate(tups);
		
		int index = 0;
		
		//PRODUCT_CAT
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File("SQLs\\" + index + "_" + tables[index] + ".SQL")));
		for(int i = 0; i<resultTest[index].length; i++){
			String insertStr = String.format("insert into %s (Name) VALUES ('%s');\n", tables[index], resultTest[index][i][0]);
			bw.write(insertStr);
		}
		bw.close();
		index++;
		
		//PRODUCT
		bw = new BufferedWriter(new FileWriter(new File("SQLs\\" + index + "_" + tables[index] + ".SQL")));
		for(int i = 0; i<resultTest[index].length; i++){
			String insertStr = String.format("insert into %s (Id, Name, Category) VALUES ('%s', '%s', '%s');\n", tables[index], resultTest[index][i][0], resultTest[index][i][1], resultTest[index][i][2]);
			bw.write(insertStr);
		}
		bw.close();
		index++;
		
		//STORE_CAT
		bw = new BufferedWriter(new FileWriter(new File("SQLs\\" + index + "_" + tables[index] + ".SQL")));
		for(int i = 0; i<resultTest[index].length; i++){
			String insertStr = String.format("insert into %s (Name) VALUES ('%s');\n", tables[index], resultTest[index][i][0]);
			bw.write(insertStr);
		}
		bw.close();
		index++;
		
		//STORE
		bw = new BufferedWriter(new FileWriter(new File("SQLs\\" + index + "_" + tables[index] + ".SQL")));
		for(int i = 0; i<resultTest[index].length; i++){
			String zip = generateZip();
			String address = generateAddress();
			String insertStr = String.format("insert into %s (Id, Address, ZipCode, Category) VALUES ('%s', '%s', '%s', '%s');\n", tables[index], resultTest[index][i][0], address, zip, resultTest[index][i][3]);
			bw.write(insertStr);
		}
		bw.close();
		index++;
		
		//CUSTOMER
		bw = new BufferedWriter(new FileWriter(new File("SQLs\\" + index + "_" + tables[index] + ".SQL")));
		for(int i = 0; i<resultTest[index].length; i++){
			
			int age = generateAge();
			String zip = generateZip();
			String gender = generateGender();
			String name = generateName(gender);
			String address = generateAddress();
			
			String insertStr = String.format("insert into %s (Id, Name, Gender, Age, Address, ZipCode) VALUES ('%s', '%s','%s', %d, '%s', '%s');\n", tables[index], resultTest[index][i][0], name, gender, age, address, zip);
			bw.write(insertStr);
		}
		bw.close();
		index++;
		
		//PRICING
		bw = new BufferedWriter(new FileWriter(new File("SQLs\\" + index + "_" + tables[index] + ".SQL")));
		for(int i = 0; i<resultTest[index].length; i++){
			String insertStr = String.format("insert into %s (ProdId, StoreId, Price, Stock) VALUES ('%s', '%s', %d.99, %d);\n", tables[index], resultTest[index][i][0], resultTest[index][i][1], (Integer)resultTest[index][i][2] % 139, (Integer)resultTest[index][i][3] % 199);
			bw.write(insertStr);
		}
		bw.close();
		index++;		
		
		//SHIPMENT_CAT
		bw = new BufferedWriter(new FileWriter(new File("SQLs\\" + index + "_" + tables[index] + ".SQL")));
		for(int i = 0; i<resultTest[index].length; i++){
			String insertStr = String.format("insert into %s (Name) VALUES ('%s');\n", tables[index], resultTest[index][i][0]);
			bw.write(insertStr);
		}
		bw.close();
		index++;
		
		//SHIPMENT
		bw = new BufferedWriter(new FileWriter(new File("SQLs\\" + index + "_" + tables[index] + ".SQL")));
		for(int i = 0; i<resultTest[index].length; i++){
			String insertStr = String.format("insert into %s (Category, ProdId, StoreId, Price) VALUES ('%s', '%s', '%s', %d.49);\n", tables[index], resultTest[index][i][0], resultTest[index][i][1], resultTest[index][i][2], (Integer)resultTest[index][i][3] % 19);
			bw.write(insertStr);
		}
		bw.close();
		index++;
		
		//PROMOTION
		bw = new BufferedWriter(new FileWriter(new File("SQLs\\" + index + "_" + tables[index] + ".SQL")));
		for(int i = 0; i<resultTest[index].length; i++){
			Pair<String, String> dates = generatePromotionDate();
			int discount = generateDiscount();
			String insertStr = String.format("insert into %s (Id, PromoCode, ShipmentCat, ProdId, StoreId, Discount, StartDate, EndDate) VALUES ('%s', '%s', '%s', '%s', '%s', %d, '%s', '%s');\n", tables[index], resultTest[index][i][0], resultTest[index][i][1], resultTest[index][i][2], resultTest[index][i][3],
					resultTest[index][i][4], discount, dates.getFirst(), dates.getSecond());
			bw.write(insertStr);
		}
		bw.close();
		index++;
		
		//PURCHASE
		bw = new BufferedWriter(new FileWriter(new File("SQLs\\" + index + "_" + tables[index] + ".SQL")));
		for(int i = 0; i<resultTest[index].length; i++){
			String date = generateSingleDate();
			String insertStr = String.format("insert into %s (CustId, ProdId, StoreId, PromoId, ShipmentCat, Feedback, Payment, Timestamp) VALUES ('%s', '%s', '%s', '%s', '%s', %d, %d.99, '%s');\n", tables[index], resultTest[index][i][0], resultTest[index][i][1], resultTest[index][i][2], resultTest[index][i][3], resultTest[index][i][4], (Integer)resultTest[index][i][5] % 6, (Integer)resultTest[index][i][6] % 139, date);
			bw.write(insertStr);
		}
		bw.close();
		index++;
		
		//B_SOCIAL
		bw = new BufferedWriter(new FileWriter(new File("SQLs\\" + index + "_" + tables[index] + ".SQL")));
		for(int i = 0; i<resultTest[index].length; i++){
			String insertStr = String.format("insert into %s (ipAddress, CustId) VALUES ('%s', '%s');\n", tables[index], resultTest[index][i][0], resultTest[index][i][1]);
			bw.write(insertStr);
		}
		bw.close();
		index++;
		
		//B_POST
		bw = new BufferedWriter(new FileWriter(new File("SQLs\\" + index + "_" + tables[index] + ".SQL")));
		for(int i = 0; i<resultTest[index].length; i++){
			String date = generateSingleDate();
			String insertStr = String.format("insert into %s (url, ipAddress, TextStr, Timestamp) VALUES ('%s', '%s', '%s', '%s');\n", tables[index], resultTest[index][i][0], resultTest[index][i][1], resultTest[index][i][2], date);
			bw.write(insertStr);
		}
		bw.close();
		index++;
		
		//B_COMMENT
		bw = new BufferedWriter(new FileWriter(new File("SQLs\\" + index + "_" + tables[index] + ".SQL")));
		for(int i = 0; i<resultTest[index].length; i++){
			String date = generateSingleDate();
			String insertStr = String.format("insert into %s (ipAddress, PostUrl, TextStr, Timestamp) VALUES ('%s', '%s', '%s', '%s');\n", tables[index], resultTest[index][i][0], resultTest[index][i][1], resultTest[index][i][2], date);
			bw.write(insertStr);
		}
		bw.close();
		index++;
		
		//F_SOCIAL
		bw = new BufferedWriter(new FileWriter(new File("SQLs\\" + index + "_" + tables[index] + ".SQL")));
		for(int i = 0; i<resultTest[index].length; i++){
			String insertStr = String.format("insert into %s (fId, CustId) VALUES ('%s', '%s');\n", tables[index], resultTest[index][i][0], resultTest[index][i][1]);
			bw.write(insertStr);
		}
		bw.close();
		index++;
		
		//F_POST
		bw = new BufferedWriter(new FileWriter(new File("SQLs\\" + index + "_" + tables[index] + ".SQL")));
		for(int i = 0; i<resultTest[index].length; i++){
			String date = generateSingleDate();
			String insertStr = String.format("insert into %s (url, fId, TextStr, Timestamp) VALUES ('%s', '%s', '%s', '%s');\n", tables[index], resultTest[index][i][0], resultTest[index][i][1], resultTest[index][i][2], date);
			bw.write(insertStr);
		}
		bw.close();
		index++;
		
		//F_COMMENT
		bw = new BufferedWriter(new FileWriter(new File("SQLs\\" + index + "_" + tables[index] + ".SQL")));
		for(int i = 0; i<resultTest[index].length; i++){
			String date = generateSingleDate();
			String insertStr = String.format("insert into %s (fId, PostUrl, TextStr, Timestamp) VALUES ('%s', '%s', '%s', '%s');\n", tables[index], resultTest[index][i][0], resultTest[index][i][1], resultTest[index][i][2], date);
			bw.write(insertStr);
		}
		bw.close();
		index++;
		
		//G_SOCIAL
		bw = new BufferedWriter(new FileWriter(new File("SQLs\\" + index + "_" + tables[index] + ".SQL")));
		for(int i = 0; i<resultTest[index].length; i++){
			String insertStr = String.format("insert into %s (gId, CustId) VALUES ('%s', '%s');\n", tables[index], resultTest[index][i][0], resultTest[index][i][1]);
			bw.write(insertStr);
		}
		bw.close();
		index++;
		
		//G_POST
		bw = new BufferedWriter(new FileWriter(new File("SQLs\\" + index + "_" + tables[index] + ".SQL")));
		for(int i = 0; i<resultTest[index].length; i++){
			String date = generateSingleDate();
			String insertStr = String.format("insert into %s (url, gId, TextStr, Timestamp) VALUES ('%s', '%s', '%s', '%s');\n", tables[index], resultTest[index][i][0], resultTest[index][i][1], resultTest[index][i][2], date);
			bw.write(insertStr);
		}
		bw.close();
		index++;
		
		//G_COMMENT
		bw = new BufferedWriter(new FileWriter(new File("SQLs\\" + index + "_" + tables[index] + ".SQL")));
		for(int i = 0; i<resultTest[index].length; i++){
			String date = generateSingleDate();
			String insertStr = String.format("insert into %s (gId, PostUrl, TextStr, Timestamp) VALUES ('%s', '%s', '%s', '%s');\n", tables[index], resultTest[index][i][0], resultTest[index][i][1], resultTest[index][i][2], date);
			bw.write(insertStr);
		}
		bw.close();
		index++;
		
		//T_SOCIAL
		bw = new BufferedWriter(new FileWriter(new File("SQLs\\" + index + "_" + tables[index] + ".SQL")));
		for(int i = 0; i<resultTest[index].length; i++){
			String insertStr = String.format("insert into %s (tId, CustId) VALUES ('%s', '%s');\n", tables[index], resultTest[index][i][0], resultTest[index][i][1]);
			bw.write(insertStr);
		}
		bw.close();
		index++;
		
		//TWEET
		bw = new BufferedWriter(new FileWriter(new File("SQLs\\" + index + "_" + tables[index] + ".SQL")));
		for(int i = 0; i<resultTest[index].length; i++){
			String date = generateSingleDate();
			String insertStr = String.format("insert into %s (tId, TextStr, Timestamp) VALUES ('%s', '%s', '%s');\n", tables[index], resultTest[index][i][0], resultTest[index][i][1], date);
			bw.write(insertStr);
		}
		bw.close();
		index++;
		
		//G_TREND
		bw = new BufferedWriter(new FileWriter(new File("SQLs\\" + index + "_" + tables[index] + ".SQL")));
		for(int i = 0; i<resultTest[index].length; i++){
			String date = generateSingleDate();
			String city = generateCity();
			String country = generateCountry();
			String word = generateWord();
			String insertStr = String.format("insert into %s (Id, Word, City, Country, Hits, Timestamp) VALUES ('%s', '%s', '%s', '%s', %d, '%s');\n", tables[index], resultTest[index][i][0], word, city, country, (Integer)resultTest[index][i][4] % (7919 * 1000), date);
			bw.write(insertStr);
		}
		bw.close();
		index++;
	}

	private static String generateWord() throws Exception {
		Random rand = new Random();
		BufferedReader br = new BufferedReader(new FileReader(new File("Data Samples\\Word.txt")));
		int line = rand.nextInt(999);
		for (int i = 0; i < line; i++) {
			br.readLine();
		}
		return br.readLine();
	}

	private static String generateCountry() throws Exception {
		Random rand = new Random();
		BufferedReader br = new BufferedReader(new FileReader(new File("Data Samples\\Country.txt")));
		int line = rand.nextInt(999);
		for (int i = 0; i < line; i++) {
			br.readLine();
		}
		return br.readLine();
	}

	private static String generateCity() throws Exception {
		Random rand = new Random();
		BufferedReader br = new BufferedReader(new FileReader(new File("Data Samples\\City.txt")));
		int line = rand.nextInt(999);
		for (int i = 0; i < line; i++) {
			br.readLine();
		}
		return br.readLine();
	}

	private static String generateAddress() throws Exception {
		Random rand = new Random();
		BufferedReader br = new BufferedReader(new FileReader(new File("Data Samples\\Address.txt")));
		int line = rand.nextInt(4999);
		for (int i = 0; i < line; i++) {
			br.readLine();
		}
		return br.readLine();
	}

	private static String generateName(String gender) throws Exception {
		String name = "";
		Random rand = new Random();
		BufferedReader br = null;
		int line = -1;
		switch (gender) {
		case "M":
			br = new BufferedReader(new FileReader(new File("Data Samples\\male_names.txt")));
			line = rand.nextInt(499);
			for (int i = 0; i < line; i++) {
				br.readLine();
			}
			name = br.readLine();
			break;
		case "F":
			br = new BufferedReader(new FileReader(new File("Data Samples\\female_names.txt")));
			line = rand.nextInt(499);
			for (int i = 0; i < line; i++) {
				br.readLine();
			}
			name = br.readLine();
			break;
		}
		
		return name;
	}

	private static String generateSingleDate() {
		Random rand = new Random();
		int randDif = rand.nextInt((int) 1.728e+9);
		Date start = new Date(Math.abs(System.currentTimeMillis() - (long)randDif * 30));
		String date_format_start = "yyyy-MM-dd HH:mm:ss";
		String startStr = new SimpleDateFormat(date_format_start).format(start);
		return startStr;
	}

	private static int generateDiscount() {
		Random rand = new Random();
		return (rand.nextInt(8) + 1) * 5;
	}

	private static Pair<String, String> generatePromotionDate() {
		Random rand = new Random();
		int randDif = rand.nextInt((int) 1.728e+9);
		int promDuration = rand.nextInt((int) 2.592e+9);
		Date start = new Date(Math.abs(System.currentTimeMillis() - (long)randDif * 30));
		Date end = new Date(start.getTime() + promDuration);
		String date_format_start = "yyyy-MM-dd 00:00:00";
		String date_format_end = "yyyy-MM-dd 23:59:59";
		String startStr = new SimpleDateFormat(date_format_start).format(start);
		String endStr = new SimpleDateFormat(date_format_end).format(end);
		return new Pair<String, String>(startStr, endStr);
	}

	private static int generateAge() {
		Random rand = new Random();
		int age = -1;
		while(age < 18){
			age = rand.nextInt(80);
		}
		return age;
	}

	private static String generateGender() {
		Random rand = new Random();
		
		if (rand.nextInt(100) > 44){
			return "M";
		}else{
			return "F";
		}
		
	}

	private static String generateZip() {
		Random rand = new Random();
		String zip = "" + rand.nextInt(10) + rand.nextInt(10) + rand.nextInt(10) + rand.nextInt(10) + rand.nextInt(10); 
		return zip;
	}

}
