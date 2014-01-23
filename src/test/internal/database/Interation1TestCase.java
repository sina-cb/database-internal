package test.internal.database;

import java.io.PrintStream;

import junit.framework.TestCase;
import internal.database.Table;

import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("rawtypes")
public class Interation1TestCase extends TestCase {

	Table table1;
	Table table2;
	PrintStream out = System.out;

	/***************************************************************************
	 * This is to setup some tables for test purpose
	 * 
	 * @author Sina, Arash, Navid, Sambitesh
	 */
	@Before
	public void setUp() throws Exception {
		table1 = new Table("movie",
				"title year length genre studioName producerNo",
				"String Integer Integer String String Integer", "title year");

		Comparable[] film0 = { "Star_Wars", 1977, 124, "sciFi", "Fox", 12345 };
		Comparable[] film1 = { "Star_Wars_2", 1980, 124, "sciFi", "Fox", 12345 };
		Comparable[] film2 = { "Rocky", 1985, 200, "action", "Universal", 12125 };
		Comparable[] film3 = { "Rambo", 1978, 100, "action", "Universal", 32355 };
		table1.insert(film0);
		table1.insert(film1);
		table1.insert(film2);
		table1.insert(film3);
		
		table2 = new Table("movie",
				"title year length genre studioName producerNo",
				"String Integer Integer String String Integer", "title year");

		Comparable[] film4 = { "Star_Wars", 1977, 124, "sciFi", "Fox", 12345 };
		Comparable[] film5 = { "Star_Wars_2", 1980, 124, "sciFi", "Fox", 12345 };
		Comparable[] film6 = { "Rocky", 1985, 200, "action", "Universal", 12125 };
		Comparable[] film7 = { "A_Separation", 2011, 123, "drama", "Sony Pictures", 12126 };
		table2.insert(film4);
		table2.insert(film5);
		table2.insert(film6);
		table2.insert(film7);
	}

	/***************************************************************************
	 * This method is used to test different parts of Iteration 1
	 * 
	 * @author Sina, Arash, Navid, Sambitesh
	 */
	@Test
	public void test() {
		projectTest();

		selectTest();
		
		unionTest();
		
		minusTest();
	}

	/*************************************************************************
	 * Test case for project method!
	 * 
	 * @author Sina, Arash, Navid, Sambitesh
	 */
	private void projectTest() {
		String projection = "title year";
		Table result = table1.project(projection);

		assertTrue("Lengths are not equal!", result.getAttribute().length == 2);

		for (String t : result.getAttribute()) {
			assertTrue(String.format(
					"This attribute does not exists in the result: %s", t),
					projection.contains(t));
		}

		projection = "year title genre length";
		result = table1.project(projection);

		assertTrue("Lengths are not equal!", result.getAttribute().length == 4);

		for (String t : result.getAttribute()) {
			assertTrue(String.format(
					"This attribute does not exists in the result: %s", t),
					projection.contains(t));
		}
	}

	/*************************************************************************
	 * Test case for project method!
	 * 
	 * @author Sina, Arash, Navid, Sambitesh
	 */
	private void selectTest() {
		Table result = table1.select("'true' == 'true'");
		assertTrue("There was an error in select method!", result.getTupleCount() == 4);
		
		result = table1.select("title == 'Star_Wars'");
		assertTrue("There was an error in select method!", result.getTupleCount() == 1);
		
		result = table2.select("year < 2000 & year > 1900 ");
		assertTrue("There was an error in select method!", result.getTupleCount() == 3);
		
		result = table2.select("title == 'Star_Wars'").select("year > 2000");
		assertTrue("There was an error in select method!", result.getTupleCount() == 0);
		
		result = table2.select("title == 'Star_Wars' | title == 'A_Separation'");
		assertTrue("There was an error in select method!", result.getTupleCount() == 2);
		
		result = table2.select("title == 'Star_Wars' & title == 'A_Separation'");
		assertTrue("There was an error in select method!", result.getTupleCount() == 0);
	}

	/*************************************************************************
	 * Test case for project method!
	 * 
	 * @author Sina, Arash, Navid, Sambitesh
	 */
	private void unionTest() {
		Table result = table1.union(table2);
		assertTrue("The table size is not true", result.getTupleCount() == 5);
		
		result = table1.union(table1);
		assertTrue("The table size is not true", result.getTupleCount() == 4);
		
		result = table2.union(table2);
		assertTrue("The table size is not true", result.getTupleCount() == 4);
	}

	/*************************************************************************
	 * Test case for project method!
	 * 
	 * @author Sina, Arash, Navid, Sambitesh
	 */
	private void minusTest() {
		Table result = table1.minus(table2);
		assertTrue("The table size is not true", result.getTupleCount() == 1);

		result = table1.minus(table1);
		assertTrue("The table size is not true", result.getTupleCount() == 0);
		
		result = table2.minus(table1);
		assertTrue("The table size is not true", result.getTupleCount() == 1);
	}

}
