import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class createData {
	public static void createProductsTable() throws SQLException{
		try(Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);){
			System.out.println("Connection to cs623_project is successful");
			try (PreparedStatement ps = conn.prepareStatement(createProduct)){
			  	 	ps.executeUpdate();
			  	System.out.println("Product table created.");
			} catch(SQLException se) {
					  	System.out.println("Product table create failed. Maybe already exist");
//					  	se.printStackTrace();
			 }
		}
	}
	
	public static void insertProductsTable() throws SQLException{
		try(Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);){
			System.out.println("Connection to cs623_project is successful");
			boolean isAutoCommit = conn.getAutoCommit();
			conn.setAutoCommit(false);
			try (PreparedStatement ps = conn.prepareStatement("INSERT INTO Product (prod, pname, price) VALUES (?, ?, ?)")){
				for (List<String> product : products) {
					ps.setString(1, product.get(0));
					ps.setString(2, product.get(1));
					ps.setFloat(3, Float.parseFloat(product.get(2)));
					ps.addBatch(); 
				}
				ps.executeBatch();
			  	System.out.println("Product table data inserted succeeded");
				conn.commit();
				conn.setAutoCommit(isAutoCommit);
			} catch(SQLException se) {
			  	System.out.println("Product table data inserted failed");
//			  	se.printStackTrace();
			  }
		}
	}

	public static void createDepotsTables() throws SQLException{
		try(Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);){
			System.out.println("Connection to cs623_project is successful");
			try (PreparedStatement ps = conn.prepareStatement(createDepot)){
			  	 	ps.executeUpdate();
			  	System.out.println("Depot table created.");
			} catch(SQLException se) {
					  	System.out.println("Depot table create failed. Maybe already exist");
//					  	se.printStackTrace();
			 }
		}
	}
	
	public static void insertDepotTable() throws SQLException{
		try(Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);){
			System.out.println("Connection to cs623_project is successful");
			boolean isAutoCommit = conn.getAutoCommit();
			conn.setAutoCommit(false);
			try (PreparedStatement ps = conn.prepareStatement("INSERT INTO Depot (dep, addr, volume) VALUES (?, ?, ?)")){
				for (List<String> depot : depots) {
					ps.setString(1, depot.get(0));
					ps.setString(2, depot.get(1));
					ps.setFloat(3, Integer.parseInt(depot.get(2)));
					ps.addBatch(); 
				}
				ps.executeBatch();
			  	System.out.println("Depot table data inserted succeeded");
				conn.commit();
				conn.setAutoCommit(isAutoCommit);
			} catch(SQLException se) {
			  	System.out.println("Depot table data inserted failed");
//			  	se.printStackTrace();
			  }
		}
	}
	
	public static void createStocksTables() throws SQLException{
		try(Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);){
			System.out.println("Connection to cs623_project is successful");
			try (PreparedStatement ps = conn.prepareStatement(createStock)){
			  	 	ps.executeUpdate();
			  	System.out.println("Stock table created.");
			} catch(SQLException se) {
					  	System.out.println("Stock table create failed. Maybe already exist");
//					  	se.printStackTrace();
			 }
		}
	}
	
	public static void insertStockTable() throws SQLException{
		try(Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);){
			System.out.println("Connection to cs623_project is successful");
			boolean isAutoCommit = conn.getAutoCommit();
			conn.setAutoCommit(false);
			try (PreparedStatement ps = conn.prepareStatement("INSERT INTO Stock (prod, dep, quantity) VALUES (?, ?, ?)")){
				for (List<String> stock : stocks) {
					ps.setString(1, stock.get(0));
					ps.setString(2, stock.get(1));
					ps.setFloat(3, Integer.parseInt(stock.get(2)));
					ps.addBatch(); 
				}
				ps.executeBatch();
			  	System.out.println("Stock table data inserted succeeded");
				conn.commit();
				conn.setAutoCommit(isAutoCommit);
			} catch(SQLException se) {
			  	System.out.println("Stock table data inserted failed");
//			  	se.printStackTrace();
			  }
		}
	}
	
	
	static final String jdbcUrl = "jdbc:postgresql://localhost/cs623_project";
	static final String jdbcUsername = "postgres";
	static final String jdbcPassword = "password";
//	static final String driver = "org.postgresql.Driver";
	static final String createProduct = "CREATE TABLE Product " +
									    "(prod VARCHAR(20) NOT NULL, " +
									    " pname VARCHAR(30), " + 
									    " price FLOAT, " + 
									    " PRIMARY KEY ( prod ))";
	static final List<String> productItem1 = List.of("p1","tape","2.5");
	static final List<String> productItem2 = List.of("p2","tv","250");
	static final List<String> productItem3 = List.of("p3","vcr","80");
	static final List<List<String>> products = List.of(productItem1,productItem2,productItem3);


	static final String createDepot =   "CREATE TABLE Depot " +
									    "(dep VARCHAR(20) NOT NULL, " +
									    " addr VARCHAR(30), " + 
									    " volume INT, " + 
									    " PRIMARY KEY ( dep ))"; 

	static final List<String> depotItem1 = List.of("d1","New York","9000");
	static final List<String> depotItem2 = List.of("d2","Syracuse","6000");
	static final List<String> depotItem3 = List.of("d4","New York","2000");
	static final List<List<String>> depots = List.of(depotItem1,depotItem2,depotItem3);
	
	static final String createStock = "CREATE TABLE Stock " +
		    "(prod VARCHAR(20) NOT NULL, " +
		    " dep VARCHAR(20) NOT NULL, " + 
		    " quantity INT, " + 
		    " PRIMARY KEY (prod, dep), " +
		    "CONSTRAINT fk_prod " +
		    "FOREIGN KEY (prod) " + 
		    "REFERENCES Product(prod) ON DELETE CASCADE, " +
		    "CONSTRAINT fk_dep " +
		    "FOREIGN KEY (dep) " +
		    "REFERENCES Depot(dep) ON DELETE CASCADE)";
	static final List<List<String>> stocks = List.of(
											 List.of("p1","d1","1000"),
											 List.of("p1","d2","100"),
											 List.of("p1","d4","1200"),
											 List.of("p3","d1","3000"),
											 List.of("p3","d4","2000"),
											 List.of("p2","d4","1500"),
											 List.of("p2","d1","400"),
											 List.of("p2","d2","2000")
											 );


}
