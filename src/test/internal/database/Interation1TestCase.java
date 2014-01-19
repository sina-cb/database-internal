package test.internal.database;

import java.io.PrintStream;

import junit.framework.TestCase;
import internal.database.Table;

import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("rawtypes")
public class Interation1TestCase extends TestCase {

	Table table;
	PrintStream out = System.out;

	@Before
	public void setUp() throws Exception {
		table = new Table("movie",
				"title year length genre studioName producerNo",
				"String Integer Integer String String Integer", "title year");

		Comparable[] film0 = { "Star_Wars", 1977, 124, "sciFi", "Fox", 12345 };
		Comparable[] film1 = { "Star_Wars_2", 1980, 124, "sciFi", "Fox", 12345 };
		Comparable[] film2 = { "Rocky", 1985, 200, "action", "Universal", 12125 };
		Comparable[] film3 = { "Rambo", 1978, 100, "action", "Universal", 32355 };
		table.insert(film0);
		table.insert(film1);
		table.insert(film2);
		table.insert(film3);
	}

	@Test
	public void test() {

		projectTest();

	}

	/*************************************************************************
	 * Test case for project method!
	 */
	private void projectTest() {
		String projection = "title year";
		Table result = table.project(projection);

		assertTrue("Lengths are not equal!", result.getAttribute().length == 2);

		for (String t : result.getAttribute()) {
			assertTrue(String.format(
					"This attribute does not exists in the result: %s", t),
					projection.contains(t));
		}

		projection = "year title genre length";
		result = table.project(projection);

		assertTrue("Lengths are not equal!", result.getAttribute().length == 4);

		for (String t : result.getAttribute()) {
			assertTrue(String.format(
					"This attribute does not exists in the result: %s", t),
					projection.contains(t));
		}
	}

	/*************************************************************************
	 * Test case for project method!
	 */
	private void selectTest() {

	}

	/*************************************************************************
	 * Test case for project method!
	 */
	private void unionTest() {

	}

	/*************************************************************************
	 * Test case for project method!
	 */
	private void minusTest() {

		
	}

}
