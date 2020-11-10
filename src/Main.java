import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Main {

	public static void main(String[] args) throws SQLException {
		setup();
		group1();
		group2();
		reset();
		group3();
		group4();
		group5();
		reset();
		group6();		
	}

	static void group1 () throws SQLException {
		final String group1Statement1 = "DELETE FROM \"stock\" WHERE prod=\'p1\';";
		final String group1Statement2 = "DELETE FROM \"product\" WHERE prod=\'p1\';";

		Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
		boolean isAutoCommit = conn.getAutoCommit();
		try {
			conn.setAutoCommit(false);
			try (PreparedStatement ps = conn.prepareStatement(group1Statement1)) {
				ps.executeUpdate();
				System.out.println(" p1 deleted from Stock.");
			} catch (SQLException se) {
				System.out.println(" p1 deleting from Stock failed.");
				se.printStackTrace();
			}
			try (PreparedStatement ps = conn.prepareStatement(group1Statement2)) {
				ps.executeUpdate();
				System.out.println(" p1 deleted from Product.");
			} catch (SQLException se) {
				System.out.println(" p1 deleting from Product failed.");
				se.printStackTrace();
			}
			conn.commit();
		} catch (SQLException se) {
			conn.rollback();
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
			try (PreparedStatement ps = conn.prepareStatement(group2Statement1)) {
				ps.executeUpdate();
				System.out.println("d1 deleted from Stock.");
			} catch (SQLException se) {
				System.out.println("d1 deleting from Stock failed.");
				se.printStackTrace();
			}
			try (PreparedStatement ps = conn.prepareStatement(group2Statement2)) {
				ps.executeUpdate();
				System.out.println("d1 deleted from Depot.");
			} catch (SQLException se) {
				System.out.println("d1 deleting from Depot failed.");
				se.printStackTrace();
			}
			conn.commit();
		} catch (SQLException se) {
			conn.rollback();
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
			try (PreparedStatement ps = conn.prepareStatement(group3Statement1)) {
				ps.executeUpdate();
			} catch (SQLException se) {
				System.out.println("Update constraint failed.");
				se.printStackTrace();
			}
			try (PreparedStatement ps = conn.prepareStatement(group3Statement2)) {
				ps.setString(1, "pp1");
				ps.executeUpdate();
				System.out.println("Updated p1 to pp1");
			} catch (SQLException se) {
				System.out.println("Update pp1 failed");
				se.printStackTrace();
			}
			conn.commit();
		} catch (SQLException se) {
			conn.rollback();
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
			try (PreparedStatement ps = conn.prepareStatement(group4Statement1)) {
				ps.executeUpdate();
			} catch (SQLException se) {
				System.out.println("Update constraint failed.");
				se.printStackTrace();
			}
			try (PreparedStatement ps = conn.prepareStatement(group4Statement2)) {
				ps.setString(1, "dd1");
				ps.executeUpdate();
				System.out.println("Updated dd1 to dd1");
			} catch (SQLException se) {
				System.out.println("Update dd1 failed");
				se.printStackTrace();
			}
			conn.commit();
		} catch (SQLException se) {
			conn.rollback();
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
			try (PreparedStatement ps = conn.prepareStatement(group5Statement1)) {
				ps.setString(1, "p100");
				ps.setString(2, "cd");
				ps.setFloat(3, 5);
				ps.executeUpdate();
				System.out.println("Insert into Product succeeded");
			} catch (SQLException se) {
				System.out.println("Insert into product failed");
				se.printStackTrace();
			}
			try (PreparedStatement ps = conn.prepareStatement(group5Statement2)) {
				ps.setString(1, "p100");
				ps.setString(2, "d2");
				ps.setInt(3, 50);
				ps.executeUpdate();
				System.out.println("Insert into Stock succeeded");
			} catch (SQLException se) {
				System.out.println("Insert into stock failed");
				se.printStackTrace();
			}
			conn.commit();
		} catch (SQLException se) {
			conn.rollback();
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
			try (PreparedStatement ps = conn.prepareStatement(group6Statement1)) {
				ps.setString(1, "d100");
				ps.setString(2, "Chicago");
				ps.setInt(3, 100);
				ps.executeUpdate();
				System.out.println("Insert into Depot succeeded");
			} catch (SQLException se) {
				System.out.println("Insert into depot failed");
				se.printStackTrace();
			}
			try (PreparedStatement ps = conn.prepareStatement(group6Statement2)) {
				ps.setString(1, "p1");
				ps.setString(2, "d100");
				ps.setInt(3, 100);
				ps.executeUpdate();
				System.out.println("Insert into Stock succeeded");
			} catch (SQLException se) {
				System.out.println("Insert into stock failed");
				se.printStackTrace();
			}
			conn.commit();
		} catch (SQLException se) {
			conn.rollback();
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
	}
	
	static void dropTables () throws SQLException {
		final String dropStatement1 = "DROP TABLE stock;";
		final String dropStatement2 = "DROP TABLE product;";
		final String dropStatement3 = "DROP TABLE depot;";

		try (Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);) {
			conn.setAutoCommit(false);
			try (PreparedStatement ps = conn.prepareStatement(dropStatement1)) {
				ps.executeUpdate();
				System.out.println(" Dropped Stock.");
			} catch (SQLException se) {
				System.out.println(" Dropping Stock failed.");
				se.printStackTrace();
			}
			try (PreparedStatement ps = conn.prepareStatement(dropStatement2)) {
				ps.executeUpdate();
				System.out.println(" Dropped Product.");
			} catch (SQLException se) {
				System.out.println(" Dropping Product failed.");
				se.printStackTrace();
			}
			try (PreparedStatement ps = conn.prepareStatement(dropStatement3)) {
				ps.executeUpdate();
				System.out.println(" Dropped Depot.");
			} catch (SQLException se) {
				System.out.println(" Dropping Depot failed.");
				se.printStackTrace();
			}
			conn.commit();
		} catch (SQLException se) {
			System.out.println("Drop execution failed for reason below:");
			se.printStackTrace();
		}
	}
	
	static void reset() throws SQLException {
		dropTables();
		setup();
	}
	
	static final String jdbcUrl = "jdbc:postgresql://localhost/cs623_project";
	static final String jdbcUsername = "postgres";
	static final String jdbcPassword = "password";
}
