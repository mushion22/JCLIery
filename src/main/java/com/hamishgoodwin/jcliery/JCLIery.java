package com.hamishgoodwin.jcliery;

import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;


import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Runs queries specified on the command line against a database and prints the results. 
 */
public class JCLIery {

	private static final Logger log = Logger.getLogger(JCLIery.class.getName());

	/**
 	  * Whether to print the column names before printing each result row 
 	  */ 
	private static boolean PRINT_COLUMNS = true;
	
	/**
 	  * Whether to enable logging output 
 	  */ 	
	private static boolean VERBOSE = true;
	
	/**
 	  * Whether to use transactions, and whether to wrap all in a single transaction or each query seperately
 	  */ 
	
	private static TransactionMode txType = TransactionMode.NONE;

	/**
 	  * Connection used to interact with database
 	  */ 
	private  Connection conn;


	/**
	 * The JDBC url for the database
	 */ 
	private String url;

	/**
	 * (Optional) username for the database
	 */ 
	private String username;

	/**
	 * (Optional) password for the database
	 */ 
	private String password;

	public JCLIery() {
	}

	public static void main(String[] args) {
		new JCLIery().doMain(args);
	}

	public void doMain(String[] args) {


		//process arguments

		// create the command line parser
		CommandLineParser parser = new BasicParser();

		// create the CLI argument option specifications
		Options options = new Options();
		options.addOption("q", "quiet", false, "Optional   Suppress all INFO/SEVERE logging output.");
		options.addOption(OptionBuilder.withLongOpt("tx")
				.withDescription("Optional  NONE (default) - No transactions. ALL = Single transaction for all queries. IDV - Each query wrapped in transaction.")
				.hasArg()
				.withArgName("TxType")
				.create("t"));
		options.addOption(OptionBuilder.withLongOpt("dburl")
				.withDescription("Mandatory  JDBC url")
				.hasArg()
				.isRequired()
				.withArgName("URL")
				.create("d"));
		options.addOption(OptionBuilder.withLongOpt("username")
				.withDescription("Optional   Database Username")
				.hasArg()
				.withArgName("username")
				.create("u"));
		options.addOption(OptionBuilder.withLongOpt("password")
				.withDescription("Optional   Database Password")
				.hasArg()
				.withArgName("password")
				.create("p"));

		options.addOption(OptionBuilder.withLongOpt("nocolumns")
				.withDescription("Optional   Suppress printing of column names")
				.create("c"));
		
		String[] filePaths = new String[]{};
		
		try {
			// parse the command line arguments
			CommandLine line = parser.parse(options, args);

			//get the argument options
			url = line.getOptionValue("dburl");
			username = line.getOptionValue("username");
			password = line.getOptionValue("password");


			//see if quiet mode has been set
			if (line.hasOption('q')) {
				VERBOSE = false;
			}
			
			//see if suppress column printout has been set
			if (line.hasOption('c')) {
				PRINT_COLUMNS = false;
			}

			//see if a transaction type has been set
			if (line.hasOption('t')) {
				String type = line.getOptionValue('t');
				try {
					txType = TransactionMode.valueOf(type);
				} catch (IllegalArgumentException e) {
					throw new ParseException("Invalid txtype: " + type);
				}
			}
			
			//the file paths will be the remaining arguments
			filePaths = line.getArgs() ;

			if (filePaths.length < 1){
				throw new ParseException("You must specify at least one file");
			}


		} catch (ParseException exp) {
			HelpFormatter formatter = new HelpFormatter();
			
			System.err.println(exp.getMessage());
			
			//print our usage help text
			formatter.printHelp("java -jar JCLIery.jar [options] file1.sql file2.sql...", options);
		
			System.exit(1);
		}

	
		List<File> files = null;



		int retCode = 0;
		try {
			//check the specified files are valid
			files = checkFiles(filePaths);

			//connect to the database
			conn = connectDB(url, username, password);
			
			if (VERBOSE) {
				log.log(Level.INFO, "Using Transaction Type: " + txType);
			}
			
			//execute the queries contained in the files
			runQueries(conn, files);


		} catch (SqueryException e) {
		
			if (VERBOSE) {
		
				log.log(Level.SEVERE, e.getMessage(), e.getCause());
			}
			
			retCode = e.getReturnCode();
		
			try {
				if (null != conn && !conn.isClosed())
					conn.rollback();
			} catch (Exception ex) {
				if (VERBOSE) {
					log.log(Level.SEVERE, "Error while rolling back transaction", ex);
				}
				retCode = 8;
			}
		}
		//disconnect the database
		if (conn != null)
			try {
				conn.close();
			} catch (Exception e) {
				if (VERBOSE) {
					log.log(Level.SEVERE, "Error while closing DB connection", e);
				}
				retCode = 9;
			}

		System.exit(retCode);
	}

	/**
	 * Read each of the files and execute them as SQL statements against the provided database connection
	 */ 
	private static void runQueries(Connection conn, List<File> files) throws SqueryException {
	
		//create a DB statement to use for execution
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			throw new SqueryException(4, "Error while creating DB statement.", e);
		}

		//read each file and execute
		for (File f : files) {
			String cmd = "";
			try {
				cmd = FileUtils.readFileToString(f);


			} catch (IOException e) {
			
				throw new SqueryException(5, "Error while reading file \"" + f.getAbsolutePath() + "\".", e);
			}

			try {

				if (VERBOSE) {
					log.info("Executing command:\n" + cmd);
				}
				//will be set to true if the query returned results, false if it was an update or other command 
				boolean isResultSet = stmt.execute(cmd);

				//commit the transaction if we're set to commit each statement individually
				if (txType == TransactionMode.IDV) {
					conn.commit();
				}
				if (VERBOSE) {
					log.info("Command execution complete.");
				}

				//if we got results (eg it was a SELECT), print them in CSV format
				if (isResultSet) {
					if (VERBOSE)
						log.info("Command returned resultset. Printing:");
					
					//get the results
					ResultSet rs = stmt.getResultSet();
					ResultSetMetaData rsmd = rs.getMetaData();

					int columnsNumber = rsmd.getColumnCount();

					//print column names if we are set to do so
					if (PRINT_COLUMNS) {
						for (int i = 1; i <= columnsNumber; i++) {
							if (i > 1)
								System.out.print(",");


							System.out.print(rsmd.getColumnName(i));
						}
						System.out.print("\n");
					}
					//iterate through each returned row, printing each field in CSV format
					while (rs.next()) {
						for (int i = 1; i <= columnsNumber; i++) {
							if (i > 1)
								System.out.print(",");

							String columnValue = rs.getString(i);

							System.out.print(columnValue);
						}

					}
				} else {
					//no results were returned
					if (VERBOSE)
						log.info("Command did not return resultset.");
					//print how many records affected. Will be 0 if it wasn't an update/delete etc.
					int updateCount = stmt.getUpdateCount();
					System.out.println(updateCount + " records affected.");


				}

			} catch (Exception e) {

				throw new SqueryException(6, "Error while executing command in file \"" + f.getAbsolutePath() + "\".", e);

			}


		}
		//commit the transaction if we are set to commit ALL 
		if (txType == TransactionMode.ALL) {
			try {
				conn.commit();
			} catch (SQLException e) {

				throw new SqueryException(7, "Error while committing all transactions", e);


			}
		}
	}

	/**
	 * Iterate through an array of file path strings, checking if they are valid and readable files. 
	 */ 
	private static List<File> checkFiles(String[] filePaths) throws SqueryException {

		//list of valid files to return
		List<File> files = new LinkedList<File>();

		if (filePaths != null) 
			for (String fileStr : filePaths) {
				File f = new File(fileStr);
				//check the path is indeed a file, and that we can read it
				if (!f.isFile()) {
					throw new SqueryException(1, "File \"" + fileStr + "\" was not a file.", null);
				}
				if (!f.canRead()) {
					throw new SqueryException(1, "File \"" + fileStr + "\" was not readable.", null);
				}

				files.add(f);
		}
		if (null == files || files.size() == 0) {
			throw new SqueryException(1, "No files were specified", null);
		}
		return files;
		
	}

	/**
	 * Check that we have the driver for the specified DB URL, then connect to that database. 
	 */ 
	private static Connection connectDB(String url, String username, String password) throws SqueryException {

		Connection conn;


		
		if (VERBOSE) {
			log.info("Connecting to database url: \"" + url + "\"" + (username == null ? "" : " with username: " + username) + (password == null ? "" : ", password: <omitted>"));
		}
		
		//check we have a driver for the given URL. This is mainly so we can give an appropriate error message if not
		try {
			Driver drv = DriverManager.getDriver(url);
		} catch (SQLException e) {

			throw new SqueryException(2, "Error loading driver for url: \"" + url + "\". Is the Driver JAR on the classpath?", e);


		}

		//try connecting to the database
		try {
			conn = DriverManager.getConnection(url, username, password);
			//we will always either not commit, or explicitly commit
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			throw new SqueryException(2, "Error connecting to  url: \"" + url + "\".", e);
		}

		return conn;
	}

	/**
 	* Enum to define whhether we should not use transactions, wrap all queries in a single transaction, 
 	* or wrap individual statments in a transaction
 	*/ 
	static enum TransactionMode {

		NONE, ALL, IDV;

	}

	/**
	 * Helper exception so we can specify a return code to exit the program with when we encounter an error
	 */ 
	private static class SqueryException extends Exception {
		private final int retCode; //exit code to use

		public SqueryException(int retCode, String message, Throwable cause) {
			super(message, cause);
			this.retCode = retCode;
		}

		/**
		 * The return code that was specified. 
		 * The program should be exited with this code after generating this exception
		 */ 
		public int getReturnCode() {
			return retCode;
		}
	}
}
