package ai.starlake.diff;

import java.util.ArrayList;

public class DBSchema {
    String catalog;
    String name;
    ArrayList<DiffAttribute> attributes;
    public DBSchema(String catalog, String name, ArrayList<DiffAttribute> attributes) {
        this.catalog = catalog;
        this.name = name;
        this.attributes = attributes;
    }
}
