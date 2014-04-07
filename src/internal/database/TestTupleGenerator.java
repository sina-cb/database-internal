package internal.database;

/*******************************************************************************
 * @file  TestTupleGenerator.java
 *
 * @author   Sadiq Charaniya, John Miller
 */

import static java.lang.System.out;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;

import com.google.common.primitives.Doubles;

/*******************************************************************************
 * This class tests the TupleGenerator on the Student Registration Database
 * defined in the Kifer, Bernstein and Lewis 2006 database textbook (see figure
 * 3.6). The primary keys (see figure 3.6) and foreign keys (see example 3.2.2)
 * are as given in the textbook.
 */
@SuppressWarnings("all")
public class TestTupleGenerator {
	/***************************************************************************
	 * The main method is the driver for TestGenerator.
	 * 
	 * @param args
	 *            the command-line arguments
	 */
	public static void main(String[] args) {
		TupleGenerator test = new TupleGeneratorImpl();

		test.addRelSchema("Student", "id name address status",
				"Integer String String String", "id", null);

		test.addRelSchema("Professor", "id name deptId",
				"Integer String String", "id", null);

		test.addRelSchema("Course", "crsCode deptId crsName descr",
				"String String String String", "crsCode", null);

		test.addRelSchema("Teaching", "crsCode semester profId",
				"String String Integer", "crsCode semester", new String[][] {
						{ "profId", "Professor", "id" },
						{ "crsCode", "Course", "crsCode" } });

		test.addRelSchema(
				"Transcript",
				"studId crsCode semester grade",
				"Integer String String String",
				"studId crsCode semester",
				new String[][] { { "studId", "Student", "id" },
						{ "crsCode", "Course", "crsCode" },
						{ "crsCode semester", "Teaching", "crsCode semester" } });

		String[] tables = { "Student", "Professor", "Course", "Transcript",
				"Teaching" };

		int tups[] = new int[] { 1000000, 1000, 2000, 50000, 5000 };

		Comparable[][][] resultTest = test.generate(tups);

		Table student = new Table("Student", "id name address status",
				"Integer String String String", "id");
		
		Table prof = new Table("Professor", "id name deptId",
				"Integer String String", "id");
		
		Table course = new Table("Course", "crsCode deptId crsName descr",
				"String String String String", "crsCode");
		
		Table trans = new Table("Transcript", "studId crsCode semester grade", 
				"Integer String String String", "studId crsCode semester");
		
		Table teaching = new Table("Teaching", "crsCode semester profId",
				"String String Integer", "crsCode semester");
		
		for (int i = 0; i < resultTest.length; i++){
			switch (i) {
			case 0: //Student
				for (int j = 0; j < resultTest[i].length; j++){
					student.insert(resultTest[i][j]);
				}
				break;
			case 1: //Prof
				for (int j = 0; j < resultTest[i].length; j++){
					prof.insert(resultTest[i][j]);
				}
				break;
			case 2: //Course
				for (int j = 0; j < resultTest[i].length; j++){
					course.insert(resultTest[i][j]);
				}
				break;
			case 3: //Trans.
				for (int j = 0; j < resultTest[i].length; j++){
					trans.insert(resultTest[i][j]);
				}
				break;
			case 4: //Teaching
				for (int j = 0; j < resultTest[i].length; j++){
					teaching.insert(resultTest[i][j]);
				}
				break;

			default:
				break;
			}
		}

		List<Double> resutls = new ArrayList<>();
		
		for (int i = 0; i < 100; i++){
			long beforeTime = System.nanoTime();
			
			Random rand = new Random();
			int id = rand.nextInt(1000000);
			
			student.select("id == " + id);
			
			long afterTime = System.nanoTime();
			
//			System.out.println(id + " ||| " + (afterTime - beforeTime) / 10E6);
			resutls.add((afterTime - beforeTime) / 10E6);
		}
		
		StandardDeviation stdDev = new StandardDeviation();
		Mean mean = new Mean();
		resutls.remove(0);
		
		double stdErr = stdDev.evaluate(Doubles.toArray(resutls)) / Math.sqrt(resutls.size());
		double average = mean.evaluate(Doubles.toArray(resutls));
		
		System.out.println("Std Err: " + stdErr);
		System.out.println("Average: " + average);
		
		/*for (int i = 0; i < resultTest.length; i++) {
			out.println(tables[i]);
			for (int j = 0; j < resultTest[i].length; j++) {
				for (int k = 0; k < resultTest[i][j].length; k++) {
					out.print(resultTest[i][j][k] + ",");
				} // for
				out.println();
			} // for
			out.println();
		} // for
*/	} // main

} // TestTupleGenerator
