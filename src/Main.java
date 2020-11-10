import java.sql.SQLException;

public class Main {

	public static void main(String[] args) throws SQLException {
		createData.createProductsTable();
		createData.insertProductsTable();
		createData.createDepotsTables();
		createData.insertDepotTable();
		createData.createStocksTables();
		createData.insertStockTable();
	}


}
