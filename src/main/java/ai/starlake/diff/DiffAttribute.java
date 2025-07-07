package ai.starlake.diff;

import java.util.ArrayList;
import ai.starlake.diff.DiffAttributeStatus;

public class DiffAttribute {
    String name;
    String typ;
    boolean array;
    DiffAttributeStatus status;

    ArrayList<DiffAttribute> attributes;  // present only if typ is "struct"

    public boolean isNestedField() {
        assert(typ.equalsIgnoreCase("struct"));
        return attributes != null && !attributes.isEmpty();
    }
    public boolean isNested() {
        return array;
    }
    public DiffAttribute(String name, String typ, boolean array, ArrayList<DiffAttribute> attributes, DiffAttributeStatus status) {
        this.name = name;
        this.typ = typ;
        this.array = array;
        this.attributes = attributes;
        this.status = status;
    }
}
