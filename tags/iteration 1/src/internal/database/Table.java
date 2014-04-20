package internal.database;

/*******************************************************************************
 * @file  Table.java
 *
 * @author   John Miller
 */

import static java.lang.System.out;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.TreeMap;

/*******************************************************************************
 * This class implements relational database tables (including attribute names,
 * domains and a list of tuples. Five basic relational algebra operators are
 * provided: project, select, union, minus and join. The insert data
 * manipulation operator is also provided. Missing are update and delete data
 * manipulation operators.
 */
@SuppressWarnings({ "rawtypes", "serial", "unchecked" })
public class Table implements Serializable, Cloneable {
	/**
	 * Debug flag, turn off once implemented
	 */
	private static final boolean DEBUG = true;

	/**
	 * Counter for naming temporary tables.
	 */
	private static int count = 0;

	/**
	 * Table name.
	 */
	private final String name;

	/**
	 * Array of attribute names.
	 */
	private final String[] attribute;

	/**
	 * Array of attribute domains: a domain may be integer types: Long, Integer,
	 * Short, Byte real types: Double, Float string types: Character, String
	 */
	private final Class[] domain;

	/**
	 * Collection of tuples (data storage).
	 */
	private final List<Comparable[]> tuples;

	/**
	 * Primary key.
	 */
	private final String[] key;

	/**
	 * Index into tuples (maps key to tuple).
	 */
	private final Map<KeyType, Comparable[]> index;

	/***************************************************************************
	 * Construct an empty table from the meta-data specifications.
	 * 
	 * @param _name
	 *            the name of the relation
	 * @param _attribute
	 *            the string containing attributes names
	 * @param _domain
	 *            the string containing attribute domains (data types)
	 * @param _key
	 *            the primary key
	 */
	public Table(String _name, String[] _attribute, Class[] _domain,
			String[] _key) {
		name = _name;
		attribute = _attribute;
		domain = _domain;
		key = _key;
		tuples = new ArrayList<>(); // also try FileList, see below
		// tuples = new FileList (this, tupleSize ());
		index = new TreeMap<>(); // also try BPTreeMap, LinHash or ExtHash
	} // Table

	/***************************************************************************
	 * Construct an empty table from the raw string specifications.
	 * 
	 * @param name
	 *            the name of the relation
	 * @param attributes
	 *            the string containing attributes names
	 * @param domains
	 *            the string containing attribute domains (data types)
	 */
	public Table(String name, String attributes, String domains, String _key) {
		this(name, attributes.split(" "), findClass(domains.split(" ")), _key
				.split(" "));

		out.println("DDL> create table " + name + " (" + attributes + ")");
	} // Table

	/***************************************************************************
	 * Construct an empty table using the meta-data of an existing table.
	 * 
	 * @param tab
	 *            the table supplying the meta-data
	 * @param suffix
	 *            the suffix appended to create new table name
	 */
	public Table(Table tab, String suffix) {
		this(tab.name + suffix, tab.attribute, tab.domain, tab.key);
	} // Table

	/***************************************************************************
	 * Project the tuples onto a lower dimension by keeping only the given
	 * attributes. Check whether the original key is included in the projection.
	 * #usage movie.project ("title year studioNo")
	 * 
	 * @param attributeList
	 *            the attributes to project onto
	 * @return the table consisting of projected tuples
	 * 
	 * @author Sina, Arash, Navid, Sambitesh
	 */
	public Table project(String attributeList) {
		out.println("RA> " + name + ".project (" + attributeList + ")");

		String[] pAttribute = attributeList.split(" ");
		int[] colPos = match(pAttribute);
		Class[] colDomain = extractDom(domain, colPos);
		String[] newKey = null;

		for (String s : this.key) {
			if (!Arrays.asList(pAttribute).contains(s)) {
				newKey = new String[1];
				break;
			}
		}

		if (newKey == null) {
			newKey = Arrays.copyOf(this.key, this.key.length);
		} else {
			newKey = Arrays.copyOf(pAttribute, pAttribute.length);
		}

		Table result = new Table(name + count++, pAttribute, colDomain, newKey);

		for (Comparable[] tup : tuples) {
			result.tuples.add(extractTup(tup, colPos));
		} // for

		return result;
	} // project

/***************************************************************************
	 * Select the tuples satisfying the given condition. A condition is written
	 * as infix expression consists of 6 comparison operators: "==", "!=", "<",
	 * "<=", ">", ">=" 2 Boolean operators: "&", "|" (from high to low
	 * precedence) #usage movie.select ("1979 < year & year < 1990")
	 * 
	 * @param condition
	 *            the check condition for tuples
	 * @return the table consisting of tuples satisfying the condition
	 * 
	 * @author Sina, Arash, Navid, Sambitesh
	 */
	public Table select(String condition) {
		out.println("RA> " + name + ".select (" + condition + ")");

		String[] postfix = infix2postfix(condition);
		Table result = new Table(name + count++, attribute, domain, key);

		for (Comparable[] tup : tuples) {
			if (evalTup(postfix, tup))
				result.tuples.add(tup);
		} // for

		return result;
	} // select

	/***************************************************************************
	 * Union this table and table2. Check that the two tables are compatible.
	 * #usage movie.union (show)
	 * 
	 * @param table2
	 *            the rhs table in the union operation
	 *            
	 * @return the table representing the union (this U table2)
	 * 
	 * @author Sina, Arash, Navid, Sambitesh
	 */
	public Table union(Table table2) {
		out.println("RA> " + name + ".union (" + table2.name + ")");

		Table result = new Table(name + count++, attribute, domain, key);

		// Compatibility check
		if (!this.compatible(table2)) {
			out.println("Incompatible Tables");
		}

		else {
			// Adds first table as it is to the result
			int length1 = this.tuples.size();
			for (int i = 0; i < length1; i++) {
				result.insert(this.tuples.get(i));
			}

			for (int i = 0; i < table2.tuples.size(); i++) {
				Comparable[] current = (Comparable[]) table2.tuples.get(i);
				Comparable[] keyVal = new Comparable[table2.key.length];
				int[] cols = match(result.key);

				for (int j = 0; j < keyVal.length; j++) {
					keyVal[j] = current[cols[j]];
				}

				// Insert only those keys which are in table2 but not in table
				// one
				if (!(result.index.containsKey(new KeyType(keyVal)))) {
					result.insert(current);
				}
			}
		}

		return result;
	} // union

	/***************************************************************************
	 * Take the difference of this table and table2. Check that the two tables
	 * are compatible. #usage movie.minus (show)
	 * 
	 * @param table2
	 *            the rhs table in the minus operation
	 *            
	 * @return the table representing the difference (this - table2)
	 * 
	 * @author Sina, Arash, Navid, Sambitesh
	 */
	public Table minus(Table table2) {
		out.println("RA> " + name + ".minus (" + table2.name + ")");

		Table result = new Table(name + count++, attribute, domain, key);

		// Compatibility check
		if (!this.compatible(table2)) {
			out.println("Incompatible Tables");
		} else {
			// Check whether tuples in Table1 are Equal to tuples in Table2 or
			// not
			for (Comparable[] tup1 : this.tuples) {
				boolean exists = false;
				for (Comparable[] tup2 : table2.tuples) {
					if (compareTuples(tup1, tup2)) {
						exists = true;
						break;
					}
				}
				// Add tuples to the result table
				if (exists == false)
					result.insert(tup1);
			}
		}

		return result;
	} // minus

	/***************************************************************************
	 * Compare two tuples deeply
	 * 
	 * @param tup1
	 *            the first tuple
	 * @param tup2
	 *            the second tuple
	 * @return true if two arrays are equal, false if those two differ
	 * 
	 * @author Sina, Arash, Navid, Sambitesh
	 */
	private boolean compareTuples(Comparable[] tup1, Comparable[] tup2) {
		for (int i = 0; i < tup1.length; i++) {
			if (tup1[i].compareTo(tup2[i]) != 0) {
				return false;
			}
		}
		return true;
	}

	/***************************************************************************
	 * Join this table and table2. If an attribute name appears in both tables,
	 * assume it is from the first table unless it is qualified with the first
	 * letter of the second table's name (e.g., "s."). In the result,
	 * disambiguate the attribute names in a similar way (e.g., prefix the
	 * second occurrence with "s_"). Caveat: the key parameter assumes joining
	 * the table with the foreign key (this) to the table containing the primary
	 * key (table2). #usage movie.join ("studioNo == name", studio); #usage
	 * movieStar.join ("name == s.name", starsIn);
	 * 
	 * @param condition
	 *            the join condition for tuples
	 * @param table2
	 *            the rhs table in the join operation
	 * @return the table representing the join (this |><| table2)
	 */
	public Table join(String condition, Table table2) {
		out.println("RA> " + name + ".join (" + condition + ", " + table2.name
				+ ")");

		Table result = new Table(name + count++, new String[0], new Class[0],
				key);

		// -----------------\\
		// TO BE IMPLEMENTED \\
		// ---------------------\\

		return result;
	} // join

	/***************************************************************************
	 * Insert a tuple to the table. #usage movie.insert ("'Star_Wars'", 1977,
	 * 124, "T", "Fox", 12345)
	 * 
	 * @param tup
	 *            the array of attribute values forming the tuple
	 * @return whether insertion was successful
	 */
	public boolean insert(Comparable[] tup) {
		out.println("DML> insert into " + name + " values ( "
				+ Arrays.toString(tup) + " )");

		if (typeCheck(tup, domain)) {
			tuples.add(tup);
			Comparable[] keyVal = new Comparable[key.length];
			int[] cols = match(key);
			for (int j = 0; j < keyVal.length; j++)
				keyVal[j] = tup[cols[j]];
			index.put(new KeyType(keyVal), tup);
			return true;
		} else {
			return false;
		} // if
	} // insert

	/***************************************************************************
	 * Get the name of the table.
	 * 
	 * @return the table's name
	 */
	public String getName() {
		return name;
	} // getName

	/***************************************************************************
	 * Print the table.
	 */
	public void print() {
		out.println("\n Table " + name);

		out.print("|-");
		for (int i = 0; i < attribute.length; i++)
			out.print("---------------");
		out.println("-|");
		out.print("| ");
		for (String a : attribute)
			out.printf("%15s", a);
		out.println(" |");

		if (DEBUG) {
			out.print("|-");
			for (int i = 0; i < domain.length; i++)
				out.print("---------------");
			out.println("-|");
			out.print("| ");
			for (Class d : domain)
				out.printf("%15s", d.getSimpleName());
			out.println(" |");
		} // if

		out.print("|-");
		for (int i = 0; i < attribute.length; i++)
			out.print("---------------");
		out.println("-|");
		for (Comparable[] tup : tuples) {
			out.print("| ");
			for (Comparable attr : tup)
				out.printf("%15s", attr);
			out.println(" |");
		} // for
		out.print("|-");
		for (int i = 0; i < attribute.length; i++)
			out.print("---------------");
		out.println("-|");
	} // print

	/***************************************************************************
	 * Determine whether the two tables (this and table2) are compatible, i.e.,
	 * have the same number of attributes each with the same corresponding
	 * domain.
	 * 
	 * @param table2
	 *            the rhs table
	 *            
	 * @return whether the two tables are compatible
	 * 
	 * @author Sina, Arash, Navid, Sambitesh
	 */
	private boolean compatible(Table table2) {
		// Two tables are union compatible if
		// 1) They have same number of columns
		// 2) Same domain type for each relative domain

		// Checking for case 1
		if (this.domain.length != table2.domain.length) {
			return false;
		}
		for (int i = 0; i < this.domain.length; i++) {
			int[] pos = new int[1];
			pos[0] = i;
			Class[] tableone = Table.extractDom(this.domain, pos);
			Class[] tabletwo = Table.extractDom(table2.domain, pos);
			// Checking for case 2
			if (tableone[0] != tabletwo[0]) {
				return (false);
			}
		}

		return true;
	} // compatible

	/***************************************************************************
	 * Return the column position for the given column/attribute name.
	 * 
	 * @param column
	 *            the given column/attribute name
	 * @return the column index position
	 */
	private int columnPos(String column) {
		for (int j = 0; j < attribute.length; j++) {
			if (column.equals(attribute[j]))
				return j;
		} // for

		out.println("columnPos: error - " + column + " not found");
		return -1; // column name not found in this table
	} // columnPos

	/***************************************************************************
	 * Return all the column positions for the given column/attribute names.
	 * 
	 * @param columns
	 *            the array of column/attribute names
	 * @return the array of column index positions
	 */
	private int[] match(String[] columns) {
		int[] colPos = new int[columns.length];

		for (int i = 0; i < columns.length; i++) {
			colPos[i] = columnPos(columns[i]);
		} // for

		return colPos;
	} // match

	/**************************************************************************
	 * Parse the operand into a Comparable object
	 * 
	 * @param inputStr
	 *            The input string
	 *            
	 * @param inputType
	 *            Class type of the operand
	 *            
	 * @return the Comparable object
	 * 
	 * @author Sina, Arash, Navid, Sambitesh
	 */
	private static Comparable parseOperand(String inputStr, Class inputType) {

		if (inputType == String.class) {
			inputStr = inputStr.replaceAll("'", "");
			return inputStr;
		}

		if (inputType == Character.class)
			return inputStr.charAt(0);

		if (inputType == Byte.class)
			return Byte.parseByte(inputStr);

		if (inputType == Short.class)
			return Short.parseShort(inputStr);

		if (inputType == Integer.class)
			return Integer.parseInt(inputStr);

		if (inputType == Long.class)
			return Long.parseLong(inputStr);

		if (inputType == Float.class)
			return Float.parseFloat(inputStr);

		if (inputType == Double.class)
			return Double.parseDouble(inputStr);

		return null;
	}

	/***************************************************************************
	 * Check whether the tuple satisfies the condition. Use a stack-based
	 * postfix expression evaluation algorithm.
	 * 
	 * @param postfix
	 *            the postfix expression for the condition
	 *            
	 * @param tup
	 *            the tuple to check
	 *            
	 * @return whether to keep the tuple
	 * 
	 * @author Sina, Arash, Navid, Sambitesh
	 */
	private boolean evalTup(String[] postfix, Comparable[] tup) {
		if (postfix == null)
			return true;
		Stack<Comparable<?>> s = new Stack<>();

		Class typeOfOperand = String.class;
		for (String token : postfix) {
			if (operator2priority(token) != 0) {
				Comparable operand1 = s.pop();
				Comparable operand2 = s.pop();
				s.push(evaluate(operand1, operand2, token));
				continue;
			}
			if (Arrays.asList(this.attribute).contains(token)) {
				s.push(tup[Arrays.asList(this.attribute).indexOf(token)]);
				typeOfOperand = this.domain[Arrays.asList(this.attribute)
						.indexOf(token)];
				continue;
			} else {
				s.push(parseOperand(token, typeOfOperand));
				typeOfOperand = String.class;
				continue;
			}
		}

		return (Boolean) s.pop();
	} // evalTup

	/***************************************************************************
	 * Pack tuple tup into a record/byte-buffer (array of bytes).
	 * 
	 * @param tup
	 *            the array of attribute values forming the tuple
	 * @return a tuple packed into a record/byte-buffer
	 * 
	 *         byte [] pack (Comparable [] tup) { byte [] record = new byte
	 *         [tupleSize ()]; byte [] b = null; int s = 0; int i = 0;
	 * 
	 *         for (int j = 0; j < domain.length; j++) { switch (domain
	 *         [j].getName ()) { case "java.lang.Integer": b =
	 *         Conversions.int2ByteArray ((Integer) tup [j]); s = 4; break; case
	 *         "java.lang.String": b = ((String) tup [j]).getBytes (); s = 64;
	 *         break;
	 * 
	 *         //-----------------\\ // TO BE IMPLEMENTED \\
	 *         //---------------------\\
	 * 
	 *         } // switch if (b == null) { out.println
	 *         ("Table.pack: byte array b is null"); return null; } // if for
	 *         (int k = 0; k < s; k++) record [i++] = b [k]; } // for return
	 *         record; } // pack
	 */

	/***************************************************************************
	 * Unpack the record/byte-buffer (array of bytes) to reconstruct a tuple.
	 * 
	 * @param record
	 *            the byte-buffer in which the tuple is packed
	 * @return an unpacked tuple
	 * 
	 *         Comparable [] unpack (byte [] record) { //-----------------\\ //
	 *         TO BE IMPLEMENTED \\ //---------------------\\
	 * 
	 *         return null; } // unpack
	 */

	/***************************************************************************
	 * Determine the size of tuples in this table in terms of the number of
	 * bytes required to store it in a record/byte-buffer.
	 * 
	 * @return the size of packed-tuples in bytes
	 * 
	 *         private int tupleSize () { int s = 0;
	 * 
	 *         for (int j = 0; j < domain.length; j++) { switch (domain
	 *         [j].getName ()) { case "java.lang.Integer": s += 4; break; case
	 *         "java.lang.String": s += 64; break;
	 * 
	 *         //-----------------\\ // TO BE IMPLEMENTED \\
	 *         //---------------------\\
	 * 
	 *         } // if } // for
	 * 
	 *         return s; } // tupleSize
	 */

	// ------------------------ Static Utility Methods
	// --------------------------

	/***************************************************************************
	 * Check the size of the tuple (number of elements in list) as well as the
	 * type of each value to ensure it is from the right domain.
	 * 
	 * @param tup
	 *            the tuple as a list of attribute values
	 * @param dom
	 *            the domains (attribute types)
	 * @return whether the tuple has the right size and values that comply with
	 *         the given domains
	 */
	private static boolean typeCheck(Comparable[] tup, Class[] dom) {
		// -----------------\\
		// TO BE IMPLEMENTED \\
		// ---------------------\\

		return true;
	} // typeCheck

	/***************************************************************************
	 * This method will take two values and apply the operator between them and
	 * return the result.
	 * 
	 * @param value1
	 *            The first value
	 * @param value2
	 *            the second value
	 * @param operator
	 *            the operator which is applied
	 * @return the result of value1 (operator) value2
	 * 
	 * @author Sina, Arash, Navid, Sambitesh
	 */
	private static Comparable evaluate(Comparable value1, Comparable value2,
			String operator) {

		switch (operator) {
		case "==":
			return (value1.compareTo(value2) == 0);
		case "!=":
			return (value1.compareTo(value2) != 0);
		case "<":
			return (value1.compareTo(value2) > 0);
		case "<=":
			return (value1.compareTo(value2) >= 0);
		case ">":
			return (value1.compareTo(value2) < 0);
		case ">=":
			return (value1.compareTo(value2) <= 0);
		case "&":
			return ((Boolean) value1 && (Boolean) value2);
		case "|":
			return ((Boolean) value1 || (Boolean) value2);
		default:
			return null;
		}

	}

	/***************************************************************************
	 * This method gets an operator string and return the priority for that
	 * operator
	 * 
	 * @param inputStr
	 *            The input operator
	 *            
	 * @return Priority for that operator
	 * 
	 * @author Sina, Arash, Navid, Sambitesh
	 */
	private static Integer operator2priority(String inputStr) {
		HashMap<String, Integer> operators = new HashMap<>();
		operators.put("==", 8);
		operators.put("!=", 7);
		operators.put("<", 6);
		operators.put("<=", 5);
		operators.put(">", 4);
		operators.put(">=", 3);
		operators.put("&", 2);
		operators.put("|", 1);

		if (operators.containsKey(inputStr)) {
			return operators.get(inputStr);
		} else {
			return 0;
		}
	}

	/***************************************************************************
	 * This method is used to convert a priority value to the corresponding
	 * operator string
	 * 
	 * @param inputInt
	 *            The priority
	 *            
	 * @return The operator string
	 * 
	 * @author Sina, Arash, Navid, Sambitesh
	 */
	private static String priority2operator(Integer inputInt) {
		HashMap<Integer, String> operators = new HashMap<>();
		operators.put(8, "==");
		operators.put(7, "!=");
		operators.put(6, "<");
		operators.put(5, "<=");
		operators.put(4, ">");
		operators.put(3, ">=");
		operators.put(2, "&");
		operators.put(1, "|");

		if (operators.containsKey(inputInt)) {
			return operators.get(inputInt);
		} else {
			return null;
		}
	}

/***************************************************************************
	 * Convert an untokenized infix expression to a tokenized postfix
	 * expression. This implementation does not handle parentheses ( ). Ex:
	 * "1979 < year & year < 1990" --> { "1979", "year", "<", "year", "1990",
	 * "<", "&" }
	 * 
	 * "==", "!=", "<",
	 * "<=", ">", ">=" 2 Boolean operators: "&", "|" (from high to low
	 * precedence
	 * 
	 * @param condition
	 *            the untokenized infix condition
	 *            
	 * @return resultant tokenized postfix expression
	 * 
	 * @author Sina, Arash, Navid, Sambitesh
	 */
	private static String[] infix2postfix(String condition) {
		if (condition == null || condition.trim() == "")
			return null;
		String[] infix = condition.split(" ");
		String[] postfix = new String[infix.length];

		Stack<Integer> operatorStack = new Stack<Integer>();
		int count = 0;
		for (String str : infix) {
			if (operator2priority(str) == 0) {
				postfix[count] = str;
				count++;
				continue;
			} else {
				if (operatorStack.isEmpty()) {
					operatorStack.push(operator2priority(str));
					continue;
				} else {
					if (operatorStack.lastElement() <= operator2priority(str)) {
						operatorStack.push(operator2priority(str));
						continue;
					} else {
						while (operatorStack.lastElement() > operator2priority(str)) {
							postfix[count] = priority2operator(operatorStack
									.pop());
							count++;
							if (operatorStack.isEmpty())
								break;
						}
						operatorStack.push(operator2priority(str));
						continue;
					}
				}
			}
		}

		while (!operatorStack.isEmpty()) {
			postfix[count] = priority2operator(operatorStack.pop());
			count++;
		}

		return postfix;
	} // infix2postfix

	/***************************************************************************
	 * Find the classes in the "java.lang" package with given names.
	 * 
	 * @param className
	 *            the array of class name (e.g., {"Integer", "String"})
	 * @return the array of Java classes for the corresponding names
	 */
	private static Class[] findClass(String[] className) {
		Class[] classArray = new Class[className.length];

		for (int i = 0; i < className.length; i++) {
			try {
				classArray[i] = Class.forName("java.lang." + className[i]);
			} catch (ClassNotFoundException ex) {
				out.println("findClass: " + ex);
			} // try
		} // for

		return classArray;
	} // findClass

	/***************************************************************************
	 * Extract the corresponding domains from the group.
	 * 
	 * @param group
	 *            where to extract from
	 * @param colPos
	 *            the column positions to extract
	 * @return the extracted domains
	 */
	private static Class[] extractDom(Class[] group, int[] colPos) {
		Class[] dom = new Class[colPos.length];

		for (int j = 0; j < colPos.length; j++) {
			dom[j] = group[colPos[j]];
		} // for

		return dom;
	} // extractDom

	/***************************************************************************
	 * Extract the corresponding attribute values from the group.
	 * 
	 * @param group
	 *            where to extract from
	 * @param colPos
	 *            the column positions to extract
	 * @return the extracted attribute values
	 */
	private static Comparable[] extractTup(Comparable[] group, int[] colPos) {
		Comparable[] tup = new Comparable[colPos.length];

		int tupIndex = 0;
		for (Integer i : colPos) {
			tup[tupIndex] = group[i];
			tupIndex++;
		}

		return tup;
	} // extractTup

	/**
	 * @return the attribute
	 * 
	 * @author Sina, Arash, Navid, Sambitesh
	 */
	public String[] getAttribute() {
		return attribute;
	}

	/**
	 * @return the domain
	 * 
	 * @author Sina, Arash, Navid, Sambitesh
	 */
	public Class[] getDomain() {
		return domain;
	}

	/****************
	 * @return Number of tuples for a Table
	 * 
	 * @author Sina, Arash, Navid, Sambitesh
	 */
	public int getTupleCount(){
		return tuples.size();
	}

} // Table class