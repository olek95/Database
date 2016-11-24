package database;

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Database {
    public static void main(String[] args) {
        String username = "olek";
        String password = "haslo12345";
        String jdbcDriver = "com.mysql.jdbc.Driver";
        String dbURL = "jdbc:mysql://localhost:3306/przykladowabaza";
        try{
            DatabaseManager manager = DatabaseManager.createDatabaseManager(jdbcDriver, dbURL, username, password);
            System.out.println(manager.min("Pracownik", "imie"));
        }catch(ClassNotFoundException | SQLException e){
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, e);
        }
    }
}
