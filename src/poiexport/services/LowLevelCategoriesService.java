package poiexport.services;

import vn.itim.engine.indexer.Poiexport;

import java.sql.*;
import java.util.List;

/**
 * Created by developer on 12/09/2015.
 */
public class LowLevelCategoriesService {

    private Connection connection;

    public LowLevelCategoriesService(Connection connection) {
        this.connection = connection;
    }

    public void saveLowLevelCategories(List<Poiexport.PoiExport.LowLevelCategory> lowLevelCategories)
            throws SQLException {
        int total = lowLevelCategories.size();
        int doneCount = 0;
        String query = "INSERT INTO `low_level_categories` VALUE (?,?,?)";
        PreparedStatement stmt;

        try {
            this.connection.setAutoCommit(false);

            for (Poiexport.PoiExport.LowLevelCategory category : lowLevelCategories) {
                stmt = this.connection.prepareStatement(query);
                stmt.setString(1, category.getNameEn());
                stmt.setString(2, category.getNameVn());
                stmt.setInt(3, category.getId());

                doneCount += stmt.executeUpdate();

                System.out.println(doneCount + "/" + total);
            }

            this.connection.commit();
        }
        catch (Exception ex) {
            this.connection.rollback();
            throw ex;
        }
    }

    public void queryLowLeveCategories()
            throws SQLException {
        String query = "SELECT * FROM `low_level_categories`";
        Statement stmt = this.connection.createStatement();
        ResultSet result = stmt.executeQuery(query);

        System.out.println("name_en / name_vn / id");
        System.out.println("\n=============\n");

        while (result.next()) {
            System.out.printf(
                    "%s / %s / %s\n",
                    result.getString("name_en"),
                    result.getString("name_vn"),
                    result.getInt("id")
            );
        }
    }

}
