package ai.starlake.openmetadata;

public class OpenMetadataJob {

    public void run(OpenMetadataConfig config) {
        if (config.option1().isEmpty())
            System.out.println("Option 1 is not defined");
        else 
            System.out.println("Option 1 is defined");
    }
}
