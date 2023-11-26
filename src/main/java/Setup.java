import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;

public class Setup {
    private static String host = null;
    private static int port = 80;
    private static String username = null;
    private static String password = null;

    private static String httpsProxy = Optional.ofNullable(System.getenv("https_proxy")).orElse("");
    private static String httpProxy = Optional.ofNullable(System.getenv("http_proxy")).orElse("");
    private static String noProxy = Optional.ofNullable(System.getenv("no_proxy")).orElse("").replaceAll(",", "|");

    private static void parseProxy(String proxy) {
        if (proxy.isEmpty()) {
            return;
        }
        if (proxy.contains("@")) {
            username = proxy.split("@")[0].split(":")[1].substring(2);
            password = proxy.split("@")[0].split(":")[2];
            final String hostAndPort = proxy.split("@")[1];
            if (hostAndPort.contains(":")) {
                host = hostAndPort.split(":")[0];
                port = Integer.parseInt(hostAndPort.split(":")[1]);
            }
            else {
                host = hostAndPort;
                port = 80;
            }
        } else {
            final String hostAndPort = proxy.split(":")[1].substring(2);
            if (hostAndPort.contains(":")) {
                host = hostAndPort.split(":")[0];
                port = Integer.parseInt(hostAndPort.split(":")[1]);
            }
            else {
                host = hostAndPort;
                port = 80;
            }
        }
    }


    private static void setJavaProxy(String protocol) {
        if (host != null) {
            System.setProperty(protocol + ".proxyHost", host);
            System.setProperty(protocol + ".proxyPort", String.valueOf(port));
        }
        if (username != null) {
            System.setProperty(protocol + ".proxyUser", username);
        }
        if (password != null) {
            System.setProperty(protocol + ".proxyPassword", password);
        }
    }
    private static void setProxy() {
        if (!httpsProxy.isEmpty()) {
            port = 443;
            parseProxy(httpsProxy);
            setJavaProxy("https");
        }
        else if (!httpProxy.isEmpty()) {
            port = 80;
            parseProxy(httpProxy);
            setJavaProxy("http");
        }
        if (!noProxy.isEmpty()) {
            System.setProperty("http.nonProxyHosts", noProxy);
        }
    }

    // ENV VARS
    public static boolean ENABLE_BIGQUERY = envIsTrue("ENABLE_BIGQUERY");
    public static boolean ENABLE_AZURE = envIsTrue("ENABLE_AZURE");
    public static boolean ENABLE_SNOWFLAKE = envIsTrue("ENABLE_SNOWFLAKE");
    public static boolean ENABLE_REDSHIFT = envIsTrue("ENABLE_REDSHIFT");
    public static boolean ENABLE_POSTGRESQL = envIsTrue("ENABLE_POSTGRESQL");

    // SPARK & STARLAKE
    private static String SCALA_VERSION = Optional.ofNullable(System.getenv("SCALA_VERSION")).orElse("2.12");
    private static String SL_VERSION = Optional.ofNullable(System.getenv("SL_VERSION")).orElse("1.0.0-SNAPSHOT");
    private static String SPARK_VERSION = Optional.ofNullable(System.getenv("SPARK_VERSION")).orElse("3.5.0");
    private static String SPARK_MAJOR_VERSION = SPARK_VERSION.split("\\.")[0];
    private static String HADOOP_VERSION = Optional.ofNullable(System.getenv("HADOOP_VERSION")).orElse("3");

    // BIGQUERY
    private static String SPARK_BQ_VERSION = Optional.ofNullable(System.getenv("SPARK_BQ_VERSION")).orElse("0.34.0");

    private static String HADOOP_AZURE_VERSION = Optional.ofNullable(System.getenv("HADOOP_AZURE_VERSION")).orElse("3.3.5");
    private static String AZURE_STORAGE_VERSION = Optional.ofNullable(System.getenv("AZURE_STORAGE_VERSION")).orElse("8.6.6");
    private static String JETTY_VERSION = Optional.ofNullable(System.getenv("JETTY_VERSION")).orElse("9.4.51.v20230217");

    // SNOWFLAKE
    private static String SNOWFLAKE_JDBC_VERSION = Optional.ofNullable(System.getenv("SNOWFLAKE_JDBC_VERSION")).orElse("3.14.0");
    private static String SPARK_SNOWFLAKE_VERSION = Optional.ofNullable(System.getenv("SPARK_SNOWFLAKE_VERSION")).orElse("3.4");

    // POSTGRESQL
    private static String POSTGRESQL_VERSION = Optional.ofNullable(System.getenv("POSTGRESQL_VERSION")).orElse("42.5.4");

    // REDSHIFT
    private static String AWS_JAVA_SDK_VERSION = Optional.ofNullable(System.getenv("AWS_JAVA_SDK_VERSION")).orElse("1.12.595");
    private static String HADOOP_AWS_VERSION = Optional.ofNullable(System.getenv("HADOOP_AWS_VERSION")).orElse("3.3.4");
    private static String REDSHIFT_JDBC_VERSION = Optional.ofNullable(System.getenv("REDSHIFT_JDBC_VERSION")).orElse("2.1.0.23");
    private static String SPARK_REDSHIFT_VERSION = Optional.ofNullable(System.getenv("SPARK_REDSHIFT_VERSION")).orElse("6.1.0-spark_3.5");
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////



    private static String SPARK_URL = "https://archive.apache.org/dist/spark/spark-" + SPARK_VERSION + "/spark-" + SPARK_VERSION + "-bin-hadoop" + HADOOP_VERSION + ".tgz";
    private static String SPARK_BQ_URL =
            "https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_" + SCALA_VERSION + "/" +
                    SPARK_BQ_VERSION +"/" +
                    "spark-bigquery-with-dependencies_"+ SCALA_VERSION+"-" + SPARK_BQ_VERSION+ ".jar";
    private static String HADOOP_AZURE_URL = "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/" + HADOOP_AZURE_VERSION + "/hadoop-azure-" + HADOOP_AZURE_VERSION + ".jar";
    private static String AZURE_STORAGE_URL = "https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/" + AZURE_STORAGE_VERSION + "/azure-storage-" + AZURE_STORAGE_VERSION + ".jar";
    private static String JETTY_URL = "https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-server/" + JETTY_VERSION + "/jetty-server-" + JETTY_VERSION + ".jar";
    private static String SNOWFLAKE_JDBC_URL = "https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/" + SNOWFLAKE_JDBC_VERSION + "/snowflake-jdbc-" + SNOWFLAKE_JDBC_VERSION + ".jar";
    private static String SPARK_SNOWFLAKE_URL = "https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_" + SCALA_VERSION +
            "/" + SCALA_VERSION + ".0-spark_" + SPARK_SNOWFLAKE_VERSION + "/spark-snowflake_" +  SCALA_VERSION + "-" + SCALA_VERSION +  ".0-spark_"+ SPARK_SNOWFLAKE_VERSION + ".jar";
    private static String POSTGRESQL_URL = "https://repo1.maven.org/maven2/org/postgresql/postgresql/" + POSTGRESQL_VERSION + "/postgresql-" + POSTGRESQL_VERSION + ".jar";

    private static String AWS_JAVA_SDK_URL = "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/" + AWS_JAVA_SDK_VERSION + "/aws-java-sdk-bundle-" + AWS_JAVA_SDK_VERSION + ".jar";
    private static String HADOOP_AWS_URL = "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/" + HADOOP_AWS_VERSION + "/hadoop-aws-" + HADOOP_AWS_VERSION + ".jar";
    private static String REDSHIFT_JDBC_URL = "https://repo1.maven.org/maven2/com/amazon/redshift/redshift-jdbc42/" + REDSHIFT_JDBC_VERSION + "/redshift-jdbc42-" + REDSHIFT_JDBC_VERSION + ".jar";
    private static String SPARK_REDSHIFT_URL = "https://repo1.maven.org/maven2/io/github/spark-redshift-community/spark-redshift_" + SCALA_VERSION +
            "/" + SPARK_REDSHIFT_VERSION + "/spark-redshift_" + SCALA_VERSION + "-" + SPARK_REDSHIFT_VERSION + ".jar";
    private static String STARLAKE_SNAPSHOT_URL = "https://s01.oss.sonatype.org/content/repositories/snapshots/ai/starlake/starlake-spark3_"+SCALA_VERSION+"/"+SL_VERSION+"/starlake-spark3_"+ SCALA_VERSION + "-" + SL_VERSION + "-assembly.jar";
    private static String STARLAKE_RELEASE_URL = "https://s01.oss.sonatype.org/content/repositories/releases/ai/starlake/starlake-spark" + SPARK_MAJOR_VERSION + "_"+SCALA_VERSION+"/"+SL_VERSION+"/starlake-spark"+SPARK_MAJOR_VERSION+"_"+ SCALA_VERSION + "-" + SL_VERSION + "-assembly.jar";

    public static String[] snowflakeURLs = {
            SNOWFLAKE_JDBC_URL,
            SPARK_SNOWFLAKE_URL
    };

    public static String[] redshiftURLs = {
            AWS_JAVA_SDK_URL,
            HADOOP_AWS_URL,
            REDSHIFT_JDBC_URL,
            SPARK_REDSHIFT_URL
    };

    public static String[] azureURLs = {
            HADOOP_AZURE_URL,
            AZURE_STORAGE_URL,
            JETTY_URL
    };

    public static String[] postgresqlURLs = {
            POSTGRESQL_URL
    };

    public static String[]  bigqueryURLs = {
            SPARK_BQ_URL
    };

    private static boolean envIsTrue(String env) {
        String value =  Optional.ofNullable(System.getenv(env)).orElse("false");
        return !value.equals("false") && !value.equals("0") ;

    }
    private static long setupTimestamp =  System.currentTimeMillis();
    private static void renameIfExists(File file) {
        if (file.exists()) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
            Date resultdate = new Date(setupTimestamp);
            file.renameTo(new File(file.getAbsolutePath() + "-" + sdf.format(resultdate)));
            System.out.println("Renamed existing file " + file.getAbsolutePath() + " to " + file.getAbsolutePath() + "-" + sdf.format(resultdate));
        }
    }
    public static void main(String[] args) throws IOException {
        try {
            if (args.length == 0) {
                System.out.println("Please specify the target directory");
                System.exit(1);
            }
            final File targetDir = new File(args[0]);
            if (!targetDir.exists()) {
                targetDir.mkdirs();
                System.out.println("Created target directory " + targetDir.getAbsolutePath());
            }

            setProxy();

            boolean anyEnabled = ENABLE_BIGQUERY || ENABLE_AZURE || ENABLE_SNOWFLAKE || ENABLE_REDSHIFT || ENABLE_POSTGRESQL;
            if (!anyEnabled) {
                ENABLE_AZURE = true;
                ENABLE_BIGQUERY = true;
                ENABLE_SNOWFLAKE = true;
                ENABLE_REDSHIFT = true;
                ENABLE_POSTGRESQL = true;
            }
            final File binDir = new File(targetDir, "bin");

            File slDir = new File(binDir, "sl");
            renameIfExists(slDir);
            slDir = new File(binDir, "sl");
            downloadAndDisplayProgress(new String[]{STARLAKE_SNAPSHOT_URL}, slDir);

            File sparkDir = new File(binDir, "spark");
            renameIfExists(sparkDir);
            sparkDir = new File(binDir, "spark");
            downloadSpark(binDir);

            File depsDir = new File(binDir, "deps");
            renameIfExists(depsDir);
            depsDir = new File(binDir, "deps");

            if (ENABLE_BIGQUERY) {
                downloadAndDisplayProgress(bigqueryURLs, depsDir);
            }
            if (ENABLE_AZURE) {
                downloadAndDisplayProgress(azureURLs, depsDir);
            }
            if (ENABLE_SNOWFLAKE) {
                downloadAndDisplayProgress(snowflakeURLs, depsDir);
            }
            if (ENABLE_REDSHIFT) {
                downloadAndDisplayProgress(redshiftURLs, depsDir);
            }
            if (ENABLE_POSTGRESQL) {
                downloadAndDisplayProgress(postgresqlURLs, depsDir);
            }
        } catch (Exception e) {
            System.out.println("Failed to download dependencies from maven central"+ e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static String getUrlName(String url) {
        return url.substring(url.lastIndexOf("/") + 1);
    }

    public static void downloadSpark(File binDir) throws IOException {
        downloadAndDisplayProgress(new String[]{SPARK_URL}, binDir);
        String tgzName = getUrlName(SPARK_URL);
        final File sparkFile = new File(binDir, tgzName);
        ProcessBuilder builder = new ProcessBuilder("tar", "-xzf", sparkFile.getAbsolutePath(), "-C", binDir.getAbsolutePath()).inheritIO();
        Process process = builder.start();
        try {
            process.waitFor();
        } catch (InterruptedException e) {
            System.out.println("Failed to extract spark tarball");
            e.printStackTrace();
        }
        sparkFile.delete();
        File sparkDir = new File(binDir, tgzName.substring(0, tgzName.lastIndexOf(".")));
        sparkDir.renameTo(new File(binDir, "spark"));
        sparkDir = new File(binDir, "spark");
        File log4j2File = new File(sparkDir, "conf/log4j2.properties.template");
        log4j2File.renameTo(new File(sparkDir, "conf/log4j2.properties"));
    }
    private static void downloadAndDisplayProgress(String[] urls, File targetDir) throws IOException {
        if (!targetDir.exists()) {
            targetDir.mkdirs();
        }
        for (String url : urls) {
            final File targetFile = new File(targetDir, getUrlName(url));
            downloadAndDisplayProgress(url, targetFile.getAbsolutePath());
        }
    }

    private static void downloadAndDisplayProgress(String urlStr, String file) throws IOException {
        final int CHUNK_SIZE = 1024;
        int filePartIndex = urlStr.lastIndexOf("/") + 1;
        String name = urlStr.substring(filePartIndex);
        String urlFolder = urlStr.substring(0, filePartIndex);
        System.out.println("Downloading to " + file + " from " + urlFolder + " ...");
        URL url = new URL(urlStr);
        URLConnection conexion = url.openConnection();
        conexion.connect();
        int lengthOfFile = conexion.getContentLength();
        System.out.println("Length of the file: " + lengthOfFile + " bytes");
        InputStream input = new BufferedInputStream(url.openStream());
        OutputStream output = new FileOutputStream(file);
        byte data[] = new byte[CHUNK_SIZE];
        long total = 0;
        int count;
        while ((count = input.read(data)) != -1) {
            total += count;
            output.write(data, 0, count);
            StringBuilder sb = new StringBuilder("Progress: " + total + " bytes");
            if (lengthOfFile > 0) {
                sb.append(" (");
                sb.append((int) (total * 100 / lengthOfFile));
                sb.append("%)");
            }
            for (int cnt = 0; cnt < sb.length(); cnt++) {
                System.out.print("\b");
            }
            System.out.print(sb.toString());
        }
        System.out.println();
        output.flush();
        output.close();
        input.close();
    }
}
