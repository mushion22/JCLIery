package com.hamishgoodwin;

import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;


import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


public class App {

	private static final Logger log = Logger.getLogger(App.class.getName());
	private static final boolean PRINT_COLUMNS = true;
	private static boolean VERBOSE = true;
	private static Connection conn;
	private static TransactionMode txType = TransactionMode.NONE;


	private String url;

	private String username;

	private String password;

	public App() {
	}

	public static void main(String[] args) {
		new App().doMain(args);
	}

	public void doMain(String[] args) {


		//process arguments

		// create the command line parser
		CommandLineParser parser = new BasicParser();

// create the Options
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

		   String[] filePaths = new String[]{};
		try {
			// parse the command line arguments
			CommandLine line = parser.parse(options, args);

			// validate that block-size has been set

			url = line.getOptionValue("dburl");
			username = line.getOptionValue("username");
			password = line.getOptionValue("password");

			filePaths = line.getArgs() ;

			if (filePaths.length < 1){
				throw new ParseException("You must specify at least one file");
			}

			if (line.hasOption('q')) {
				VERBOSE = false;
			}

			if (line.hasOption('t')) {
				String type = line.getOptionValue('t');
				try {
					txType = TransactionMode.valueOf(type);
				} catch (IllegalArgumentException e) {
					throw new ParseException("Invalid txtype: " + type);
				}
			}


		} catch (ParseException exp) {
			HelpFormatter formatter = new HelpFormatter();
			System.err.println(exp.getMessage());
			formatter.printHelp("java -jar Squery.jar [options] file1.sql file2.sql...", options);
			System.exit(1);
		}

		if (VERBOSE) {
			log.log(Level.INFO, "Using Transaction Type: " + txType);
		}
		List<File> files = null;



		int retCode = 0;
		try {
			files = checkFiles(filePaths);


			conn = connectDB(url, username, password);

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

	private static void runQueries(Connection conn, List<File> files) throws SqueryException {
		//execute each file
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			throw new SqueryException(4, "Error while creating DB statement.", e);
		}

		for (File f : files) {
			String cmd = "";
			try {
				cmd = FileUtils.readFileToString(f);


			} catch (IOException e) {
				if (VERBOSE) {
					log.log(Level.SEVERE, "Error while reading file \"" + f.getAbsolutePath() + "\".", e);
				}
				throw new SqueryException(5, "Error while reading file \"" + f.getAbsolutePath() + "\".", e);
			}

			try {

				if (VERBOSE) {
					log.info("Executing command:\n" + cmd);
				}
				boolean isResultSet = stmt.execute(cmd);

				if (txType == TransactionMode.IDV) {
					conn.commit();
				}
				if (VERBOSE) {
					log.info("Command execution complete.");
				}

				if (isResultSet) {
					if (VERBOSE)
						log.info("Command returned resultset. Printing:");
					ResultSet rs = stmt.getResultSet();
					ResultSetMetaData rsmd = rs.getMetaData();

					int columnsNumber = rsmd.getColumnCount();

					//print column names
					if (PRINT_COLUMNS) {
						for (int i = 1; i <= columnsNumber; i++) {
							if (i > 1)
								System.out.print(",");


							System.out.print(rsmd.getColumnName(i));
						}
						System.out.print("\n");
					}

					while (rs.next()) {
						for (int i = 1; i <= columnsNumber; i++) {
							if (i > 1)
								System.out.print(",");

							String columnValue = rs.getString(i);

							System.out.print(columnValue);
						}

					}
				} else {
					if (VERBOSE)
						log.info("Command did not return resultset.");
					int updateCount = stmt.getUpdateCount();
					System.out.println(updateCount + " records affected.");


				}

			} catch (Exception e) {

				throw new SqueryException(6, "Error while executing command in file \"" + f.getAbsolutePath() + "\".", e);

			}


		}
		if (txType == TransactionMode.ALL) {
			try {
				conn.commit();
			} catch (SQLException e) {

				throw new SqueryException(7, "Error while committing all transactions", e);


			}
		}
	}

	private static List<File> checkFiles(String[] filePaths) throws SqueryException {


		List<File> files = new LinkedList<File>();

		if (filePaths != null) for (String fileStr : filePaths) {

			File f = new File(fileStr);

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
		//check whether the files are valid
	}

	private static Connection connectDB(String url, String username, String password) throws SqueryException {

		Connection conn;

		if (VERBOSE) {
			log.info("Connecting to database url: \"" + url + "\"" + (username == null ? "" : " with username: " + username) + (password == null ? "" : ", password: <omitted>"));
		}
		try {
			Driver drv = DriverManager.getDriver(url);
		} catch (SQLException e) {

			throw new SqueryException(2, "Error loading driver for url: \"" + url + "\". Is the Driver JAR on the classpath?", e);


		}

		//try connecting to the database
		try {
			conn = DriverManager.getConnection(url, username, password);
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			throw new SqueryException(2, "Error connecting to  url: \"" + url + "\".", e);
		}

		return conn;
	}

	static enum TransactionMode {

		NONE, ALL, IDV;


	}

	private static class SqueryException extends Exception {
		private int retCode; //exit code to use

		public SqueryException(int retCode, String message, Throwable cause) {
			super(message, cause);
			this.retCode = retCode;
		}

		public int getReturnCode() {
			return retCode;
		}
	}
}
