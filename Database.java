package database;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Klasa <code>Database</code> reprezentuje testy służące do sprawdzenia poprawności 
 * metod klasy DatabaseManager. Jeśli tabela istnieje, zostaje usuwana na początku i 
 * tworzona na nowo, aby można było zobaczyć efekty zmian podczas testowania. 
 * @author AleksanderSklorz
 */
public class Database {
    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        System.out.print("Podaj nazwę użytkownika: ");
        String username = in.next();
        System.out.print("Podaj hasło: ");
        String password = in.next();
        System.out.print("Podaj adres URL (np. jdbc:mysql://localhost:3306/przykladowabaza): ");
        String dbURL = in.next() + "?dontTrackOpenResources=true";
        try{
            DatabaseManager manager = DatabaseManager.createDatabaseManager();
            manager.createConnection(dbURL, username, password);
            if(manager.exists("Pracownik")) manager.executeUpdate("DROP TABLE Pracownik");
            manager.executeUpdate("CREATE TABLE Pracownik(id INT, imie VARCHAR(20), nazwisko VARCHAR(20), data_urodzenia DATE, zarobek DOUBLE, PRIMARY KEY(id))");
            int insertedRows = manager.executeUpdate("INSERT INTO Pracownik VALUES(1, 'Mateusz', 'Serwan', '1995-01-01', 1500.50)");
            insertedRows += manager.executeUpdate("INSERT INTO Pracownik VALUES(2, 'Janina', 'Niedziela', '1960-05-01', 2000)");
            insertedRows += manager.executeUpdate("INSERT INTO Pracownik VALUES(3, 'Wojciech', 'Janosz', '1995-11-21', 1600.25)");
            insertedRows += manager.executeUpdate("INSERT INTO Pracownik VALUES(4, 'Jakub', 'Miczołek', '1995-11-23', 2000)");
            System.out.println("Liczba wierszych dodanych po stworzeniu tabeli: " + insertedRows);
            System.out.println("Początkowy stan tabeli: ");
            ArrayList<String> rows = manager.getAllFromResultSet(manager.getAll("Pracownik"));
            if(rows != null) for(String row : rows) System.out.println(row);
            manager.executeUpdatePreparedStatement("UPDATE Pracownik SET imie = ? WHERE imie = ?", "Andrzej", "Jakub");
            System.out.println("Lista imion po zamianie imienia Jakub na Andrzej:");
            rows = manager.getAllFromResultSet(manager.executeQuery("SELECT imie FROM Pracownik"));
            if(rows != null) for(String row : rows) System.out.println(row);
            System.out.println("Suma wszystkich zarobków: " + manager.sum("Pracownik", "zarobek"));
            System.out.println("Średnia zarobków: " + manager.average("Pracownik", "zarobek"));
            System.out.println("Ostatnie alfabetycznie imię: " + manager.max("Pracownik", "imie"));
            System.out.println("Pierwsze alfabetycznie imię: " + manager.min("Pracownik", "imie"));
            System.out.println("Liczba kolumn: " + manager.countColumns("Pracownik"));
            System.out.println("Liczba wierszy: " + manager.countRows("Pracownik"));
        }catch(ClassNotFoundException | SQLException e){
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, e);
        }
    }
}
