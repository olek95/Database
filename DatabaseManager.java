package database;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;

public class DatabaseManager{
    private static DatabaseManager manager;
    private static Connection conn;
    private DatabaseManager(String jdbcDriver, String url, String user, String password) throws ClassNotFoundException, SQLException{
        Class.forName(jdbcDriver);
        conn = DriverManager.getConnection(url, user, password);
    }
    /**
     * Tworzy zarządcę bazy danych, jeżeli taki zarządca nie został jeszcze utworzony. 
     * @param jdbcDriver sterownik bazy danych 
     * @param url adres url bazy danych 
     * @param user nazwa użytkownika 
     * @param password hasło użytkownika 
     * @return zarządcę bazy danych 
     * @throws ClassNotFoundException
     * @throws SQLException 
     */
    public static synchronized DatabaseManager createDatabaseManager(String jdbcDriver, String url, String user, String password) throws ClassNotFoundException, SQLException{
        if(manager == null) manager = new DatabaseManager(jdbcDriver, url, user, password); 
        return manager;
    }
    /**
     * Wykonuje polecenie SQL, które zwraca zbiór wynikowy. Typowo do tego rodzaju 
     * poleceń należy polecenie SELECT. 
     * @param query zwracające zbiór wynikowy polecenie SQL (typowo SELECT), które ma zostać wykonane
     * @return zbiór wyników polecenia SQL
     * @throws SQLException 
     */
    public ResultSet executeQuery(String query) throws SQLException{
        Statement stat = conn.createStatement();
        return stat.executeQuery(query);
    }
    /**
     * Wykonuje polecenie SQL, które zwraca ilość zmienionych wierszy lub 
     * zwracają nic. Typowo do tego rodzaju poleceń należą polecenia DML (UPDATE, 
     * INSERT, DELETE) lub DDL.
     * @param query zwracające liczbę zmienionych wierszy lub nic nie zwracające polecenie SQL (DML lub DDL), które ma zostać wykonane
     * @return liczba zmienionych wierszy lub 0 jeśli polecenie nic nie zwraca
     * @throws SQLException 
     */
    public int executeUpdate(String query) throws SQLException{
        Statement stat = conn.createStatement();
        return stat.executeUpdate(query);
    }
    public boolean execute(String query) throws SQLException{
        Statement stat = conn.createStatement(); 
        return stat.execute(query);
    }
    /**
     * Zwraca zbiór wszystkich wierszy tabeli. 
     * @param tableName nazwa tabeli 
     * @return zbiór wierszy tabeli
     * @throws SQLException 
     */
    public ResultSet getAll(String tableName) throws SQLException{
        return executeQuery("SELECT * FROM " + tableName); 
    }
    /**
     * Zwraca listę wierszy odczytanych ze zbioru wyników. 
     * @param rs zbiór wyników
     * @return lista wierszy ze zbioru wyników
     * @throws SQLException 
     */
    public ArrayList<String> getAllFromResultSet(ResultSet rs) throws SQLException{
        ArrayList<String> resultSetContent = new ArrayList();
        int columnsNumber = rs.getMetaData().getColumnCount();
        String row;
        while(rs.next()){
            row = "";
            for(int i = 1; i <= columnsNumber; i++)
                row += rs.getString(i) + " ";
            resultSetContent.add(row);
        }
        return resultSetContent;
    }
    /**
     * Wykonuje przygotowane polecenie SQL, który zwraca zbiór wynikowy. Typowo 
     * dla tego rodzaju poleceń należy polecenie SELECT. Jest to polecenie 
     * przygotowane, a więc możliwe jest użycie zmiennych wartości danego atrybutu. 
     * @param query zwracające zbiór wynikowy przygotowane polecenie SQL (typowo SELECT), które ma zostać wykonane
     * @param values wartości wstawiane w miejsce ? w poleceniu przygotowanym. Kolejność podania wartości jest równa kolejności wstawiania 
     * @return zbiór wynikowy dla przygotowanego polecenia SQL 
     * @throws SQLException 
     */
    public ResultSet executeQueryInPreparedStatement(String query, Object... values) throws SQLException{
        PreparedStatement stat = conn.prepareStatement(query);
        for(int i = 1; i <= values.length; i++){
            stat.setString(i, values[i-1].toString());
        }
        return stat.executeQuery();
    }
    /**
     * Wykonuje przygotowane polecenie SQL, które zwraca ilość zmienionych wierszy lub 
     * zwracają nic. Typowo do tego rodzaju poleceń należą polecenia DML (UPDATE, 
     * INSERT, DELETE) lub DDL. Jest to polecenie przygotowane, a więc możliwe jest 
     * użycie zmiennych wartości danego atrybutu. 
     * @param query zwracające liczbę zmienionych wierszy lub nic nie zwracające przygotowane polecenie SQL (DML lub DDL), które ma zostać wykonane
     * @param values wartości wstawiane w miejsce ? w poleceniu przygotowanym. Kolejność podania wartości jest równa kolejności wstawiania
     * @return liczba zmienionych wierszy lub 0 jeśli polecenie nic nie zwraca
     * @throws SQLException 
     */
    public int executeUpdatePreparedStatement(String query, Object... values) throws SQLException{
        PreparedStatement stat = conn.prepareStatement(query);
        for(int i = 1; i <= values.length; i++){
            stat.setString(i, values[i-1].toString());
        }
        return stat.executeUpdate();
    }
    /**
     * Oblicza sumę wartości (całkowitych lub zmiennoprzecinkowych) dla danej kolumny tabeli. 
     * W przypadku próby dodania wartości innego typu, zgłasza wyjątek ArithmeticException. 
     * @param tableName nazwa tabeli
     * @param columnName nazwa kolumny z wartościami liczbowymi
     * @return suma danych liczbowych w kolumnie 
     * @throws SQLException 
     */
    public double sum(String tableName, String columnName) throws SQLException{
        if(isNumericType(tableName, columnName))
            return Double.valueOf(getAllFromResultSet(executeQuery("SELECT SUM(" + columnName + ") FROM " + tableName)).get(0));
        throw new ArithmeticException("Możesz dodać tylko liczby!");
    }
    /**
     * Oblicza średnią wartości (całkowitych lub zmiennoprzecinkowych) dla danej kolumny tabeli. 
     * W przypadku próby dodania wartości innego typu, zgłasza wyjątek ArithmeticException. 
     * @param tableName nazwa tabeli 
     * @param columnName nazwa kolumny z wartościami liczbowymi. 
     * @return średnia danych liczbowych w kolumnie
     * @throws SQLException 
     */
    public double average(String tableName, String columnName) throws SQLException{
        if(isNumericType(tableName, columnName))
            return Double.valueOf(getAllFromResultSet(executeQuery("SELECT AVG(" + columnName + ") FROM " + tableName)).get(0));
        throw new ArithmeticException("Możesz policzyć średnią tylko z liczb!"); 
    }
    /**
     * Znajduje maksymalną wartość dowolnego typu w danej kolumnie. 
     * @param tableName nazwa tabeli 
     * @param columnName nazwa kolumny
     * @return maksymalna wartość w kolumnie 
     * @throws SQLException 
     */
    public String max(String tableName, String columnName) throws SQLException{
        return getAllFromResultSet(executeQuery("SELECT MAX(" + columnName + ") FROM " + tableName)).get(0);
    }
    /**
     * Znajduje minimalną wartość dowolnego typu w danej kolumnie. 
     * @param tableName nazwa tabeli 
     * @param columnName nazwa kolumny
     * @return minimalna wartość w kolumnie 
     * @throws SQLException 
     */
    public String min(String tableName, String columnName) throws SQLException{
        return getAllFromResultSet(executeQuery("SELECT MIN(" + columnName + ") FROM " + tableName)).get(0);
    }
    /**
     * Sprawdza czy dana kolumna tabeli jest typu liczbowego (całkowitego lub zmiennoprzecinkowego).
     * @param tableName nazwa tabeli
     * @param columnName nazwa kolumny do sprawdzenia 
     * @return true jeśli kolumna jest typu liczbowego, false w przeciwnym przypadku 
     * @throws SQLException 
     */
    public boolean isNumericType(String tableName, String columnName) throws SQLException{
        int[] numericTypes = {Types.FLOAT, Types.REAL, Types.DOUBLE, Types.NUMERIC, 
            Types.DECIMAL, Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT};
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet rs = metaData.getColumns(null, null, tableName, columnName);
        rs.next(); 
        int type = rs.getInt("DATA_TYPE");
        for(int i = 0; i < numericTypes.length; i++)
            if(type == numericTypes[i]) return true;
        return false;
    }
}
