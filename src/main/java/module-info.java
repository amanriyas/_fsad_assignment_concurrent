module com.example.fsad_assignment {
    requires javafx.controls;
    requires javafx.fxml;
    requires java.sql;
    requires java.sql.rowset;


    opens com.example.fsad_assignment to javafx.fxml;
    exports com.example.fsad_assignment;
}