package ai.starlake.diff;

import java.util.ArrayList;


public class Attribute {
    String name;
    String typ;
    boolean array;
    AttributeStatus status;

    ArrayList<Attribute> attributes;  // present only if typ is "struct"

    public boolean isNestedField() {
        assert(typ.equalsIgnoreCase("struct"));
        return attributes != null && !attributes.isEmpty();
    }
    public boolean isNested() {
        return array;
    }
    public Attribute(String name, String typ, boolean array, ArrayList<Attribute> attributes, AttributeStatus status) {
        this.name = name;
        this.typ = typ;
        this.array = array;
        this.attributes = attributes;
        this.status = status;
    }
}
