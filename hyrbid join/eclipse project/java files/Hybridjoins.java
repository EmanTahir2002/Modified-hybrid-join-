package intro;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;


import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

import java.util.HashMap;
import java.util.Map;
import java.util.*;

import java.util.Scanner;

public class Hybridjoins {

    

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter your username: ");
        String username = scanner.nextLine();

        System.out.print("Enter your password: ");
        String password = scanner.nextLine();

        // Assuming you have the JDBC URL constant
        String jdbcUrl = "jdbc:mysql://localhost:3306/electronica_dw";
        LinkedList<Integer> queues = new LinkedList<>();

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             Statement stmt = conn.createStatement()) {

            
            String strSelect = "SELECT ProductID, CustomerName, Quantity_Ordered, Order_ID, CustomerID, Gender FROM transactions ";
            System.out.println("The records selected are:");
            int rowCount = 0;

            // Define a multi-value hash map for transactions
            MultiValuedMap<Integer, Object> multiMap = new ArrayListValuedHashMap<>();

            try (ResultSet rs = stmt.executeQuery(strSelect)) {
                while (rs.next() && rowCount < 100) { 
                    int productId = rs.getInt("ProductID");
                    String customerName = rs.getString("CustomerName");
                    int quantityOrdered = rs.getInt("Quantity_Ordered");
                    int orderID = rs.getInt("Order_ID");
                    int customerID = rs.getInt("CustomerID");
                    String gender = rs.getString("Gender");

                    // Store data in the multi-value map
                    multiMap.put(productId, customerName);
                    multiMap.put(productId, quantityOrdered);
                    multiMap.put(productId, orderID);
                    multiMap.put(productId, customerID);
                    multiMap.put(productId, gender);

                    queues.add(productId);

                    System.out.println("ProductID: " + productId + ", CustomerName: " + customerName + ", Quantity: " + quantityOrdered);
                    ++rowCount;
                }
            }

            System.out.println("Original Queue: " + queues);

            while (!queues.isEmpty()) {
            	
            	//removing the head of queue
                int currentProductId = queues.poll();
                System.out.println("\nProcessing ProductID: " + currentProductId);

                //Fetching 10 tuples from the "master_data" table for the current product ID and the next 9
                String strMSelect = "SELECT productID, productName, productPrice, supplierID, supplierName, storeID, storeName FROM master_data WHERE productID >= " + currentProductId + " LIMIT 10";
                int rc = 0; 

                //Define a map to store master data with product IDs as keys and the remaining tuple as multivalues
                Map<Integer, Object[]> masterDataMap = new HashMap<>();

                try (ResultSet rsMasterData = stmt.executeQuery(strMSelect)) {
                	
                    while (rsMasterData.next() && rc < 10) { 
                    	
                        int productId = rsMasterData.getInt("productID");
                        String productName = rsMasterData.getString("productName");
                        double productPrice = rsMasterData.getDouble("productPrice");
                        int supplierID = rsMasterData.getInt("supplierID");
                        String supplierName = rsMasterData.getString("supplierName");
                        int storeID = rsMasterData.getInt("storeID");
                        String storeName = rsMasterData.getString("storeName");

                        //Store data in the map with product ID as the key
                        Object[] productData = {productName, productPrice, supplierID, supplierName, storeID, storeName};
                        masterDataMap.put(productId, productData);

                        System.out.println("Master Data - ProductID: " + productId + ", ProductName: " + productName + ", ProductPrice: " + productPrice + ", Store id: "+ storeID );
                        ++rc;
                    }
                }

                //Now Iterate over the master data map and keep checking for matches
                for (Map.Entry<Integer, Object[]> masterEntry : masterDataMap.entrySet()) {
                    int masterProductId = masterEntry.getKey();
                    Object[] masterDataTuple = masterEntry.getValue();

                    //Check if the current master product ID exists in transactions
                    if (multiMap.containsKey(masterProductId)) {
                        System.out.println("\nMatch found for Master ProductID: " + masterProductId);

                       
                        System.out.println("Master Data Tuple: " + Arrays.toString(masterDataTuple));

                        
                        List<Object> transactions = new ArrayList<>(multiMap.get(masterProductId));
                        System.out.println("Transactional Tuple: " + transactions);

                        // Establish a new connection for dimension tables, wanted to use another port for writing to the schema
                        String dimJdbcUrl = "jdbc:mysql://localhost:3306/electronica_dw"; 
                        String dimUsername = "root";
                        String dimPassword = "root";

                        //inserting values to dimension tables
                        try (Connection dimConn = DriverManager.getConnection(dimJdbcUrl, dimUsername, dimPassword)) {
                            String insertProductDimSql = "INSERT INTO product_dim (productID, productName, productPrice) VALUES (?, ?, ?)";

                            try (PreparedStatement dimStmt = dimConn.prepareStatement(insertProductDimSql)) {
                            	
                                //Set parameters for the prepared statement
                                dimStmt.setInt(1, masterProductId);
                                dimStmt.setObject(2, masterDataTuple[0]); //product name is at index 0
                                dimStmt.setObject(3, masterDataTuple[1]); //product price is at index 1

                                // Execute the update
                                dimStmt.executeUpdate();
                            } 
                            	catch (SQLException e) {
                            e.printStackTrace(); // Handle the exception appropriately
                        }
                    
                                
                                //the same is repeated for the remaining dimension tables 
                                String insertCustomerDimSql = "INSERT INTO customer_dim (CustomerID, CustomerName, Gender) VALUES (?, ?, ?)";
                                try (PreparedStatement dimStmt1 = dimConn.prepareStatement(insertCustomerDimSql)) {
                                    dimStmt1.setInt(1, (int) transactions.get(3)); 
                                    dimStmt1.setString(2, (String) transactions.get(0)); 
                                    dimStmt1.setString(3, (String) transactions.get(4)); 

                                    dimStmt1.executeUpdate();
                                } catch (SQLException ex) {
                                    ex.printStackTrace();
                                }
                                
                                
                                
                                String insertSupplierDimSql = "INSERT INTO supplier_dim (supplierID, supplierName) VALUES (?, ?)";
                                try (PreparedStatement dimStmt1 = dimConn.prepareStatement(insertSupplierDimSql)) {
                                    dimStmt1.setInt(1, (int) masterDataTuple[2]); 
                                    dimStmt1.setString(2, (String) masterDataTuple[3]);

                                    dimStmt1.executeUpdate();
                                } catch (SQLException ex) {
                                    ex.printStackTrace();
                                }

                                
                                
                                String insertStoreDimSql = "INSERT INTO store_dim (storeID, storeName) VALUES (?, ?)";
                                try (PreparedStatement dimStmt1 = dimConn.prepareStatement(insertStoreDimSql)) {
                                    dimStmt1.setInt(1, (int) masterDataTuple[4]); 
                                    dimStmt1.setString(2, (String) masterDataTuple[5]); 

                                    dimStmt1.executeUpdate();
                                } catch (SQLException ex) {
                                    ex.printStackTrace();
                                }
                                
                                
                                
                                String insertFactSql = "INSERT INTO Sales_fact ( Customer_ID, Supplier_ID, Store_ID, Product_ID, Quantity, Amount) VALUES (?, ?, ?, ?, ?, ?)";

                                try (PreparedStatement factStmt = dimConn.prepareStatement(insertFactSql)) {
                                    
                                    factStmt.setInt(1, (int) transactions.get(3)); 
                                    factStmt.setInt(2, (int) masterDataTuple[2]); 
                                    factStmt.setInt(3, (int) masterDataTuple[4]); 
                                    factStmt.setInt(4, masterProductId);
                                    factStmt.setInt(5, (int) transactions.get(1)); 
                                    double productPrice = (double) masterDataTuple[1];
                                    
                                    //calculating amount
                                    double amount = (int) transactions.get(1) * productPrice;

                                    factStmt.setDouble(6, amount); 
                                    factStmt.executeUpdate();
                                }

                    }
                    }
                    else {
                        System.out.println("No match found for Master ProductID: " + masterProductId);
                    }
                }
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
}
