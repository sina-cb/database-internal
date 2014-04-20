package internal.database;

public class MyTupleGenerator {

	public static void main(String[] args) {

		TupleGenerator test = new TupleGeneratorImpl();
		
		test.addRelSchema("Professor", "id name deptId",
				"Integer String String", "id", null);

		test.addRelSchema("Course", "crsCode deptId crsName descr",
				"String String String String", "crsCode", null);

		test.addRelSchema("Teaching", "crsCode semester profId",
				"String String Integer", "crsCode semester", new String[][] {
				{ "profId", "Professor", "id" },
				{ "crsCode", "Course", "crsCode" } });

		String[] tables = { "Professor", "Course", "Teaching" };
		int tups[] = new int[] { 1, 1, 1 };
		
		Comparable[][][] resultTest = test.generate(tups);
		
		System.out.print(resultTest);
		
	}

}
