import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

public class Main {

	public static void main(String[] args) throws SQLException {
		createDatabaseIfNotExists();
		group1AndGroup2();
		group3AndGroup4();
		group5AndGroup6();
	}
	
	static void group1AndGroup2() throws SQLException{
		setup();
		group1();
		group2();
		System.out.println("----------------------------");
		System.out.println("Result for group1 and group2");
		showTheResult();
		dropTables();
	}
	
	static void group3AndGroup4() throws SQLException{
		setup();
		group3();
		group4();
		System.out.println("----------------------------");
		System.out.println("Result for group3 and group4");
		showTheResult();
		dropTables();
	}
	
	static void group5AndGroup6() throws SQLException{
		setup();
		group5();
		group6();
		System.out.println("----------------------------");
		System.out.println("Result for group5 and group6");
		showTheResult();
		dropTables();
	}

	static void group1 () throws SQLException {
		final String group1Statement1 = "DELETE FROM \"stock\" WHERE prod=\'p1\';";
		final String group1Statement2 = "DELETE FROM \"product\" WHERE prod=\'p1\';";

		Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
		boolean isAutoCommit = conn.getAutoCommit();
		try {
			conn.setAutoCommit(false);
			try (PreparedStatement ps = conn.prepareStatement(group1Statement1);
				 PreparedStatement pps = conn.prepareStatement(group1Statement2)) {
				ps.executeUpdate();
				System.out.println("p1 deleted from Stock.");
				pps.executeUpdate();
				System.out.println("p1 deleted from Product.");
			} catch (SQLException se) {
				conn.rollback();
				System.out.println("p1 deleting from Stock and Product failed: ");
				se.printStackTrace();
			}
			conn.commit();
		} catch (SQLException se) {
			System.out.println("Group 1 execution failed for reason below:");
			se.printStackTrace();
		} finally {
		    conn.setAutoCommit(isAutoCommit);
		    conn.close();
		}
	}
	
	static void group2 () throws SQLException {
		final String group2Statement1 = "DELETE FROM stock WHERE dep=\'d1\';";
		final String group2Statement2 = "DELETE FROM depot WHERE dep=\'d1\';";

		Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
		boolean isAutoCommit = conn.getAutoCommit();
		try {
			conn.setAutoCommit(false);
			try (PreparedStatement ps = conn.prepareStatement(group2Statement1);
				 PreparedStatement pps = conn.prepareStatement(group2Statement2)) {
				ps.executeUpdate();
				System.out.println("p1 deleted from Stock.");
				pps.executeUpdate();
				System.out.println("d1 deleted from Depot.");
			} catch (SQLException se) {
				conn.rollback();
				System.out.println("d1 deleting from Stock and Depot failed: ");
				se.printStackTrace();
			}
			conn.commit();
		} catch (SQLException se) {
			System.out.println("Group 2 execution failed for reason below:");
			se.printStackTrace();
		} finally {
		    conn.setAutoCommit(isAutoCommit);
		    conn.close();
		}
	}
	
	static void group3 () throws SQLException {
		final String group3Statement1 = "ALTER TABLE stock DROP CONSTRAINT fk_prod, ADD CONSTRAINT fk_prod FOREIGN KEY (prod) REFERENCES Product(prod) ON UPDATE CASCADE;";
		final String group3Statement2 = "UPDATE product SET prod=? WHERE prod=\'p1\';";

		Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
		boolean isAutoCommit = conn.getAutoCommit();
		try {
			conn.setAutoCommit(false);
			try (PreparedStatement ps = conn.prepareStatement(group3Statement1);
				 PreparedStatement pps = conn.prepareStatement(group3Statement2)) {
				ps.executeUpdate();
				System.out.println("Update constraint for on Update Cascade succeeded");
				pps.setString(1, "pp1");
				pps.executeUpdate();
				System.out.println("Update pp1 succeeded");
			} catch (SQLException se) {
				conn.rollback();
				System.out.println("Update pp1 failed: ");
				se.printStackTrace();
			}
			conn.commit();
		} catch (SQLException se) {
			System.out.println("Group 3 execution failed for reason below:");
			se.printStackTrace();
		} finally {
		    conn.setAutoCommit(isAutoCommit);
		    conn.close();
		}
	}
	
	static void group4 () throws SQLException {
		final String group4Statement1 = "ALTER TABLE stock DROP CONSTRAINT fk_dep, ADD CONSTRAINT fk_dep FOREIGN KEY (dep) REFERENCES Depot(dep) ON UPDATE CASCADE;";
		final String group4Statement2 = "UPDATE depot SET dep=? WHERE dep='d1';";

		Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
		boolean isAutoCommit = conn.getAutoCommit();
		try {
			conn.setAutoCommit(false);
			try (PreparedStatement ps = conn.prepareStatement(group4Statement1);
				 PreparedStatement pps = conn.prepareStatement(group4Statement2)) {
				ps.executeUpdate();
				System.out.println("Update constraint for on Update Cascade succeeded");
				pps.setString(1, "dd1");
				pps.executeUpdate();
				System.out.println("Update dd1 succeeded");
			} catch (SQLException se) {
				conn.rollback();
				System.out.println("Update dd1 failed: ");
				se.printStackTrace();
			}
			conn.commit();
		} catch (SQLException se) {
			System.out.println("Group 4 execution failed for reason below:");
			se.printStackTrace();
		} finally {
		    conn.setAutoCommit(isAutoCommit);
		    conn.close();
		}
	}
	
	static void group5 () throws SQLException {
		final String group5Statement1 = "INSERT INTO Product (prod, pname, price) VALUES (?, ?, ?);";
		final String group5Statement2 = "INSERT INTO Stock (prod, dep, quantity) VALUES (?, ?, ?);";

		Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
		boolean isAutoCommit = conn.getAutoCommit();
		try {
			conn.setAutoCommit(false);
			try (PreparedStatement ps = conn.prepareStatement(group5Statement1);
				 PreparedStatement pps = conn.prepareStatement(group5Statement2)) {
				ps.setString(1, "p100");
				ps.setString(2, "cd");
				ps.setFloat(3, 5);
				ps.executeUpdate();
				System.out.println("Insert into Product succeeded");
				pps.setString(1, "p100");
				pps.setString(2, "d2");
				pps.setInt(3, 50);
				pps.executeUpdate();
				System.out.println("Insert into Stock succeeded");
			} catch (SQLException se) {
				conn.rollback();
				System.out.println("Insert into Stock and Product failed: ");
				se.printStackTrace();
			}
			conn.commit();
		} catch (SQLException se) {
			System.out.println("Group 5 execution failed for reason below:");
			se.printStackTrace();
		} finally {
		    conn.setAutoCommit(isAutoCommit);
		    conn.close();
		}
	}
	
	static void group6 () throws SQLException {
		final String group6Statement1 = "INSERT INTO Depot (dep, addr, volume) VALUES (?, ?, ?);";
		final String group6Statement2 = "INSERT INTO Stock (prod, dep, quantity) VALUES (?, ?, ?);";

		Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
		boolean isAutoCommit = conn.getAutoCommit();
		try {
			conn.setAutoCommit(false);
			try (PreparedStatement ps = conn.prepareStatement(group6Statement1);
				 PreparedStatement pps = conn.prepareStatement(group6Statement2)) {
				ps.setString(1, "d100");
				ps.setString(2, "Chicago");
				ps.setInt(3, 100);
				ps.executeUpdate();
				System.out.println("Insert into Depot succeeded");
				ps.setString(1, "p1");
				ps.setString(2, "d100");
				ps.setInt(3, 100);
				ps.executeUpdate();
				System.out.println("Insert into Stock succeeded");
			} catch (SQLException se) {
				conn.rollback();
				System.out.println("Insert into Stock and Depot failed: ");
				se.printStackTrace();
			}
			conn.commit();
		} catch (SQLException se) {
			System.out.println("Group 6 execution failed for reason below:");
			se.printStackTrace();
		} finally {
		    conn.setAutoCommit(isAutoCommit);
		    conn.close();
		}
	}
	
	static void setup() throws SQLException {
		createData.createProductsTable();
		createData.insertProductsTable();
		createData.createDepotsTables();
		createData.insertDepotTable();
		createData.createStocksTables();
		createData.insertStockTable();
		System.out.println("Setup successfully");
	}
	
	static void dropTables () throws SQLException {
		final String dropStatement1 = "DROP TABLE stock;";
		final String dropStatement2 = "DROP TABLE product;";
		final String dropStatement3 = "DROP TABLE depot;";

		try (Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)) {
			conn.setAutoCommit(false);
			try (PreparedStatement ps = conn.prepareStatement(dropStatement1);
				 PreparedStatement pps = conn.prepareStatement(dropStatement2);
				 PreparedStatement pss = conn.prepareStatement(dropStatement3)) {
				ps.executeUpdate();
				System.out.println(" Dropped Stock.");
				pps.executeUpdate();
				System.out.println(" Dropped Product.");
				pss.executeUpdate();
				System.out.println(" Dropped Depot.");
			} catch (SQLException se) {
				conn.rollback();
				System.out.println(" Dropping tables failed.");
				se.printStackTrace();
			}
			conn.commit();
		} catch (SQLException se) {
			System.out.println("Drop execution failed for reason below:");
			se.printStackTrace();
		}
	}
	
	static void createDatabaseIfNotExists() throws SQLException {
		final String jdbcUrlNoDatabase = "jdbc:postgresql://localhost/";
		final String cDINE1 = "SELECT datname FROM pg_catalog.pg_database WHERE datname = ?";
		final String cDINE2 = "CREATE DATABASE cs623_project;";

		
		try (Connection conn = DriverManager.getConnection(jdbcUrlNoDatabase, jdbcUsername, jdbcPassword);){
			try (PreparedStatement ps = conn.prepareStatement(cDINE1)){
				ps.setString(1, "cs623_project");
				try (ResultSet rs = ps.executeQuery()){
					if (!rs.next()) {
						try (PreparedStatement pps = conn.prepareStatement(cDINE2)){
							pps.executeUpdate();
							System.out.println("cs623_project database created");
						}
					} else {
						System.out.println("cs623_project database already exist");
					}
				}
			}
		}
	}
	
	static void showTheResult() throws SQLException {
		final String queryStock = "SELECT * FROM stock";
		final String queryProduct = "SELECT * FROM product";
		final String queryDepot = "SELECT * FROM depot";
		
		try(Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);){
			try (PreparedStatement ps = conn.prepareStatement(queryStock)){
				try (ResultSet rs = ps.executeQuery()){
					ResultSetMetaData resultSetMetaData  = rs.getMetaData();
					int ColumnCount = resultSetMetaData .getColumnCount();
			        int[] columnMaxLengths = new int[ColumnCount];
			        ArrayList<String[]> results = new ArrayList<>();

					while (rs.next()) {
			            String[] columnStr = new String[ColumnCount];

						for(int i = 0 ; i < ColumnCount; i++){
			                columnStr[i] = rs.getString(i + 1);
			                columnMaxLengths[i] = Math.max(columnMaxLengths[i], (columnStr[i] == null) ? 0 : columnStr[i].length());
			                columnMaxLengths[i] = Math.max(columnMaxLengths[i], resultSetMetaData.getColumnName(i+1).length());
						}
			            results.add(columnStr);
					}
					printer.printSeparator(columnMaxLengths);
			        printer.printColumnName(resultSetMetaData, columnMaxLengths);
			        printer.printSeparator(columnMaxLengths);
			        Iterator<String[]> iterator = results.iterator();
			        String[] columnStr;
			        while (iterator.hasNext()) {
			            columnStr = iterator.next();
			            for (int i = 0; i < ColumnCount; i++) {
			                // System.out.printf("|%" + (columnMaxLengths[i] + 1) + "s", columnStr[i]);
			                System.out.printf("|%" + columnMaxLengths[i] + "s", columnStr[i]);
			            }
			            System.out.println("|");
			        }
			        printer.printSeparator(columnMaxLengths);
					}
			}
			
			try (PreparedStatement ps = conn.prepareStatement(queryProduct)){
				try (ResultSet rs = ps.executeQuery()){
					ResultSetMetaData resultSetMetaData  = rs.getMetaData();
					int ColumnCount = resultSetMetaData .getColumnCount();
			        int[] columnMaxLengths = new int[ColumnCount];
			        ArrayList<String[]> results = new ArrayList<>();

					while (rs.next()) {
			            String[] columnStr = new String[ColumnCount];

						for(int i = 0 ; i < ColumnCount; i++){
			                columnStr[i] = rs.getString(i + 1);
			                columnMaxLengths[i] = Math.max(columnMaxLengths[i], (columnStr[i] == null) ? 0 : columnStr[i].length());
			                columnMaxLengths[i] = Math.max(columnMaxLengths[i], resultSetMetaData.getColumnName(i+1).length());
						}
			            results.add(columnStr);
					}
					printer.printSeparator(columnMaxLengths);
			        printer.printColumnName(resultSetMetaData, columnMaxLengths);
			        printer.printSeparator(columnMaxLengths);
			        Iterator<String[]> iterator = results.iterator();
			        String[] columnStr;
			        while (iterator.hasNext()) {
			            columnStr = iterator.next();
			            for (int i = 0; i < ColumnCount; i++) {
			                // System.out.printf("|%" + (columnMaxLengths[i] + 1) + "s", columnStr[i]);
			                System.out.printf("|%" + columnMaxLengths[i] + "s", columnStr[i]);
			            }
			            System.out.println("|");
			        }
			        printer.printSeparator(columnMaxLengths);
					}
			}
			
			try (PreparedStatement ps = conn.prepareStatement(queryDepot)){
				try (ResultSet rs = ps.executeQuery()){
					ResultSetMetaData resultSetMetaData  = rs.getMetaData();
					int ColumnCount = resultSetMetaData .getColumnCount();
			        int[] columnMaxLengths = new int[ColumnCount];
			        ArrayList<String[]> results = new ArrayList<>();

					while (rs.next()) {
			            String[] columnStr = new String[ColumnCount];

						for(int i = 0 ; i < ColumnCount; i++){
			                columnStr[i] = rs.getString(i + 1);
			                columnMaxLengths[i] = Math.max(columnMaxLengths[i], (columnStr[i] == null) ? 0 : columnStr[i].length());
			                columnMaxLengths[i] = Math.max(columnMaxLengths[i], resultSetMetaData.getColumnName(i+1).length());
						}
			            results.add(columnStr);
					}
					printer.printSeparator(columnMaxLengths);
			        printer.printColumnName(resultSetMetaData, columnMaxLengths);
			        printer.printSeparator(columnMaxLengths);
			        Iterator<String[]> iterator = results.iterator();
			        String[] columnStr;
			        while (iterator.hasNext()) {
			            columnStr = iterator.next();
			            for (int i = 0; i < ColumnCount; i++) {
			                // System.out.printf("|%" + (columnMaxLengths[i] + 1) + "s", columnStr[i]);
			                System.out.printf("|%" + columnMaxLengths[i] + "s", columnStr[i]);
			            }
			            System.out.println("|");
			        }
			        printer.printSeparator(columnMaxLengths);
					System.out.println("----------------------------");
					}
			}
		}

	}
	
	
	static final String jdbcUrl = "jdbc:postgresql://localhost/cs623_project";
	static final String jdbcUsername = "postgres";
	static final String jdbcPassword = "password";
}
