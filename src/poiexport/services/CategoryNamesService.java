package poiexport.services;

import vn.itim.engine.indexer.Poiexport;

import java.sql.*;
import java.util.List;

/**
 * Created by developer on 12/09/2015.
 */
public class CategoryNamesService {

    private Connection connection;

    public CategoryNamesService(Connection connection) {
        this.connection = connection;
    }

    public void saveCategoryNames(List<Poiexport.PoiExport.CategoryName> categoryNames)
            throws SQLException {
        int total = categoryNames.size();
        int doneCount = 0;
        String query = "INSERT INTO `category_names` VALUE (?,?,?,?,?)";
        PreparedStatement stmt = null;

        try {
            this.connection.setAutoCommit(false);

            for (Poiexport.PoiExport.CategoryName categoryName : categoryNames) {
                stmt = this.connection.prepareStatement(query);
                stmt.setString(1, categoryName.getCategoryNameEn());
                stmt.setString(2, categoryName.getCategoryNameVn());
                stmt.setInt(3, categoryName.getCategory());
                stmt.setInt(4, categoryName.getAdsCategory());
                stmt.setString(5, categoryName.getAdsCategoryName());

                doneCount += stmt.executeUpdate();

                System.out.println(doneCount + "/" + total);
            }

            this.connection.commit();
        } catch (Exception ex) {
            this.connection.rollback();
            throw ex;
        }
    }

    public void queryCategoryNames() throws SQLException {
        String query = "SELECT * FROM `category_names`";
        Statement stmt = this.connection.createStatement();
        ResultSet result;

        result = stmt.executeQuery(query);

        while (result.next()) {
            System.out.println("English: " + result.getString("category_name_en"));
            System.out.println("Vietnamese: " + result.getString("category_name_vn"));
            System.out.println("Category: " + result.getInt("category"));
            System.out.println("Ads Category: " + result.getInt("ads_category"));
            System.out.println("Ads Category Name: " + result.getString("ads_category_name"));
            System.out.println("\n\t=============\n");
        }
    }

}
