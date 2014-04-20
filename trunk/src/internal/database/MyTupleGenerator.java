package internal.database;

public class MyTupleGenerator {

	public static void main(String[] args) {

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
		
		test.addRelSchema("CUSTOMER", "CustId sex age CustAddress CustZip",
				"String String Integer String String", "CustId", null);
		
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
		
		test.addRelSchema("B_COMMENT", "ipAddress PostUrl TextStr",
				"String String String", "ipAddress PostUrl TextStr", new String[][] {
				{ "ipAddress", "B_SOCIAL", "ipAddress" },
				{ "PostUrl", "B_POST", "url" }});
		
		test.addRelSchema("F_SOCIAL", "fId CustId",
				"String String", "fId", new String[][] {
				{ "CustId", "CUSTOMER", "CustId" }});
		
		test.addRelSchema("F_POST", "url fId TextStr Timestamp",
				"String String String String", "url", new String[][] {
				{ "fId", "F_SOCIAL", "fId" }});
		
		test.addRelSchema("F_COMMENT", "fId PostUrl TextStr",
				"String String String", "fId PostUrl TextStr", new String[][] {
				{ "fId", "F_SOCIAL", "fId" },
				{ "PostUrl", "F_POST", "url" }});
		
		test.addRelSchema("G_SOCIAL", "gId CustId",
				"String String", "gId", new String[][] {
				{ "CustId", "CUSTOMER", "CustId" }});
		
		test.addRelSchema("G_POST", "url gId TextStr Timestamp",
				"String String String String", "url", new String[][] {
				{ "gId", "G_SOCIAL", "gId" }});
		
		test.addRelSchema("G_COMMENT", "gId PostUrl TextStr",
				"String String String", "gId PostUrl TextStr", new String[][] {
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
		int tups[] = new int[] { 2, 10, 2, 10, 12, 50, 2, 10, 10, 50, 5, 10, 20, 5, 10, 20, 5, 10, 20, 5, 20, 20};
		
		Comparable[][][] resultTest = test.generate(tups);
		
		int index = 0;
		
		//PRODUCT_CAT
		for(int i = 0; i<resultTest[index].length; i++){
			for (int j = 0; j<resultTest[index][i].length; j++){
				System.out.print(resultTest[index][i][j] + " ");
			}
			System.out.println();
		}
		index++;
		
		System.out.println("-----------------------------------------");
		//PRODUCT
		for(int i = 0; i<resultTest[index].length; i++){
			for (int j = 0; j<resultTest[index][i].length; j++){
				System.out.print(resultTest[index][i][j] + " ");
			}
			System.out.println();
		}
		index++;
		
		System.out.println("-----------------------------------------");
		//STORE_CAT
		for(int i = 0; i<resultTest[index].length; i++){
			for (int j = 0; j<resultTest[index][i].length; j++){
				System.out.print(resultTest[index][i][j] + " ");
			}
			System.out.println();
		}
		index++;
		
		System.out.println("-----------------------------------------");
		//STORE
		for(int i = 0; i<resultTest[index].length; i++){
			for (int j = 0; j<resultTest[index][i].length; j++){
				System.out.print(resultTest[index][i][j] + " ");
			}
			System.out.println();
		}
		index++;
		
		System.out.println("-----------------------------------------");
		//CUSTOMER
		for(int i = 0; i<resultTest[index].length; i++){
			for (int j = 0; j<resultTest[index][i].length; j++){
				System.out.print(resultTest[index][i][j] + " ");
			}
			System.out.println();
		}
		index++;
		
		System.out.println("-----------------------------------------");
		//PRICING
		for(int i = 0; i<resultTest[index].length; i++){
			for (int j = 0; j<resultTest[index][i].length; j++){
				System.out.print(resultTest[index][i][j] + " ");
			}
			System.out.println();
		}
		index++;		
		
		System.out.println("-----------------------------------------");
		//SHIPMENT_CAT
		for(int i = 0; i<resultTest[index].length; i++){
			for (int j = 0; j<resultTest[index][i].length; j++){
				System.out.print(resultTest[index][i][j] + " ");
			}
			System.out.println();
		}
		index++;
		
		System.out.println("-----------------------------------------");
		//SHIPMENT
		for(int i = 0; i<resultTest[index].length; i++){
			for (int j = 0; j<resultTest[index][i].length; j++){
				System.out.print(resultTest[index][i][j] + " ");
			}
			System.out.println();
		}
		index++;
		
		System.out.println("-----------------------------------------");
		//PROMOTION
		for(int i = 0; i<resultTest[index].length; i++){
			for (int j = 0; j<resultTest[index][i].length; j++){
				System.out.print(resultTest[index][i][j] + " ");
			}
			System.out.println();
		}
		index++;
		
		System.out.println("-----------------------------------------");
		//PURCHASE
		for(int i = 0; i<resultTest[index].length; i++){
			for (int j = 0; j<resultTest[index][i].length; j++){
				System.out.print(resultTest[index][i][j] + " ");
			}
			System.out.println();
		}
		index++;
		
		System.out.println("-----------------------------------------");
		//B_SOCIAL
		for(int i = 0; i<resultTest[index].length; i++){
			for (int j = 0; j<resultTest[index][i].length; j++){
				System.out.print(resultTest[index][i][j] + " ");
			}
			System.out.println();
		}
		index++;
		
		System.out.println("-----------------------------------------");
		//B_POST
		for(int i = 0; i<resultTest[index].length; i++){
			for (int j = 0; j<resultTest[index][i].length; j++){
				System.out.print(resultTest[index][i][j] + " ");
			}
			System.out.println();
		}
		index++;
		
		System.out.println("-----------------------------------------");
		//B_COMMENT
		for(int i = 0; i<resultTest[index].length; i++){
			for (int j = 0; j<resultTest[index][i].length; j++){
				System.out.print(resultTest[index][i][j] + " ");
			}
			System.out.println();
		}
		index++;
		
		System.out.println("-----------------------------------------");
		//F_SOCIAL
		for(int i = 0; i<resultTest[index].length; i++){
			for (int j = 0; j<resultTest[index][i].length; j++){
				System.out.print(resultTest[index][i][j] + " ");
			}
			System.out.println();
		}
		index++;
		
		System.out.println("-----------------------------------------");
		//F_POST
		for(int i = 0; i<resultTest[index].length; i++){
			for (int j = 0; j<resultTest[index][i].length; j++){
				System.out.print(resultTest[index][i][j] + " ");
			}
			System.out.println();
		}
		index++;
		
		System.out.println("-----------------------------------------");
		//F_COMMENT
		for(int i = 0; i<resultTest[index].length; i++){
			for (int j = 0; j<resultTest[index][i].length; j++){
				System.out.print(resultTest[index][i][j] + " ");
			}
			System.out.println();
		}
		index++;
		
		System.out.println("-----------------------------------------");
		//G_SOCIAL
		for(int i = 0; i<resultTest[index].length; i++){
			for (int j = 0; j<resultTest[index][i].length; j++){
				System.out.print(resultTest[index][i][j] + " ");
			}
			System.out.println();
		}
		index++;
		
		System.out.println("-----------------------------------------");
		//G_POST
		for(int i = 0; i<resultTest[index].length; i++){
			for (int j = 0; j<resultTest[index][i].length; j++){
				System.out.print(resultTest[index][i][j] + " ");
			}
			System.out.println();
		}
		index++;
		
		System.out.println("-----------------------------------------");
		//G_COMMENT
		for(int i = 0; i<resultTest[index].length; i++){
			for (int j = 0; j<resultTest[index][i].length; j++){
				System.out.print(resultTest[index][i][j] + " ");
			}
			System.out.println();
		}
		index++;
		
		System.out.println("-----------------------------------------");
		//T_SOCIAL
		for(int i = 0; i<resultTest[index].length; i++){
			for (int j = 0; j<resultTest[index][i].length; j++){
				System.out.print(resultTest[index][i][j] + " ");
			}
			System.out.println();
		}
		index++;
		
		System.out.println("-----------------------------------------");
		//TWEET
		for(int i = 0; i<resultTest[index].length; i++){
			for (int j = 0; j<resultTest[index][i].length; j++){
				System.out.print(resultTest[index][i][j] + " ");
			}
			System.out.println();
		}
		index++;
		
		System.out.println("-----------------------------------------");
		//G_TREND
		for(int i = 0; i<resultTest[index].length; i++){
			for (int j = 0; j<resultTest[index][i].length; j++){
				System.out.print(resultTest[index][i][j] + " ");
			}
			System.out.println();
		}
		index++;
	}

}
