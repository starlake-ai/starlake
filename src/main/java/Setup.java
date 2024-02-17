import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class Setup {

    private static class JarDependency {

        private final String url;

        private final String artefactName;

        public JarDependency(String artefactName, String url) {
            this.url = url;
            this.artefactName = artefactName;
        }

        public String getUrlName() {
            return url.substring(url.lastIndexOf("/") + 1);
        }
    }

    private static String host = null;
    private static int port = 80;
    private static String username = null;
    private static String password = null;

    private static String httpsProxy = getEnv("https_proxy").orElse("");
    private static String httpProxy = getEnv("http_proxy").orElse("");
    private static String noProxy = getEnv("no_proxy").orElse("").replaceAll(",", "|");

    private static boolean isWindowsOs() {
        String os = System.getProperty("os.name").toLowerCase();
        return os.startsWith("windows");
    }

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
            } else {
                host = hostAndPort;
            }
        } else {
            final String hostAndPort = proxy.split(":")[1].substring(2);
            if (hostAndPort.contains(":")) {
                host = hostAndPort.split(":")[0];
                port = Integer.parseInt(hostAndPort.split(":")[1]);
            } else {
                host = hostAndPort;
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
        } else if (!httpProxy.isEmpty()) {
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
    private static final String SCALA_VERSION = getEnv("SCALA_VERSION").orElse("2.12");
    private static final String SL_VERSION = getEnv("SL_VERSION").orElse("1.0.1-SNAPSHOT");
    private static final String SPARK_VERSION = getEnv("SPARK_VERSION").orElse("3.5.0");
    private static final String SPARK_MAJOR_VERSION = SPARK_VERSION.split("\\.")[0];
    private static final String HADOOP_VERSION = getEnv("HADOOP_VERSION").orElse("3");

    // BIGQUERY
    private static final String SPARK_BQ_VERSION = getEnv("SPARK_BQ_VERSION").orElse("0.35.1");

    private static final String HADOOP_AZURE_VERSION = getEnv("HADOOP_AZURE_VERSION").orElse("3.3.5");
    private static final String AZURE_STORAGE_VERSION = getEnv("AZURE_STORAGE_VERSION").orElse("8.6.6");
    private static final String JETTY_VERSION = getEnv("JETTY_VERSION").orElse("9.4.51.v20230217");

    // SNOWFLAKE
    private static final String SNOWFLAKE_JDBC_VERSION = getEnv("SNOWFLAKE_JDBC_VERSION").orElse("3.14.0");
    private static final String SPARK_SNOWFLAKE_VERSION = getEnv("SPARK_SNOWFLAKE_VERSION").orElse("3.4");

    // POSTGRESQL
    private static final String POSTGRESQL_VERSION = getEnv("POSTGRESQL_VERSION").orElse("42.5.4");

    // REDSHIFT
    private static final String AWS_JAVA_SDK_VERSION = getEnv("AWS_JAVA_SDK_VERSION").orElse("1.12.595");
    private static final String HADOOP_AWS_VERSION = getEnv("HADOOP_AWS_VERSION").orElse("3.3.4");
    private static final String REDSHIFT_JDBC_VERSION = getEnv("REDSHIFT_JDBC_VERSION").orElse("2.1.0.23");
    private static final String SPARK_REDSHIFT_VERSION = getEnv("SPARK_REDSHIFT_VERSION").orElse("6.1.0-spark_3.5");
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


    private static final JarDependency SPARK_JAR = new JarDependency("spark", "https://archive.apache.org/dist/spark/spark-" + SPARK_VERSION + "/spark-" + SPARK_VERSION + "-bin-hadoop" + HADOOP_VERSION + ".tgz");
    private static final JarDependency SPARK_BQ_JAR = new JarDependency("spark-bigquery-with-dependencies",
            "https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_" + SCALA_VERSION + "/" +
                    SPARK_BQ_VERSION + "/" +
                    "spark-bigquery-with-dependencies_" + SCALA_VERSION + "-" + SPARK_BQ_VERSION + ".jar");
    private static final JarDependency HADOOP_AZURE_JAR = new JarDependency("hadoop-azure", "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/" + HADOOP_AZURE_VERSION + "/hadoop-azure-" + HADOOP_AZURE_VERSION + ".jar");
    private static final JarDependency AZURE_STORAGE_JAR = new JarDependency("azure-storage", "https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/" + AZURE_STORAGE_VERSION + "/azure-storage-" + AZURE_STORAGE_VERSION + ".jar");
    private static final JarDependency JETTY_SERVER_JAR = new JarDependency("jetty-server", "https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-server/" + JETTY_VERSION + "/jetty-server-" + JETTY_VERSION + ".jar");
    private static final JarDependency SNOWFLAKE_JDBC_JAR = new JarDependency("snowflake-jdbc", "https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/" + SNOWFLAKE_JDBC_VERSION + "/snowflake-jdbc-" + SNOWFLAKE_JDBC_VERSION + ".jar");
    private static final JarDependency SPARK_SNOWFLAKE_JAR = new JarDependency("spark-snowflake", "https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_" + SCALA_VERSION +
            "/" + SCALA_VERSION + ".0-spark_" + SPARK_SNOWFLAKE_VERSION + "/spark-snowflake_" + SCALA_VERSION + "-" + SCALA_VERSION + ".0-spark_" + SPARK_SNOWFLAKE_VERSION + ".jar");
    private static final JarDependency POSTGRESQL_JAR = new JarDependency("postgresql", "https://repo1.maven.org/maven2/org/postgresql/postgresql/" + POSTGRESQL_VERSION + "/postgresql-" + POSTGRESQL_VERSION + ".jar");

    private static final JarDependency AWS_JAVA_SDK_JAR = new JarDependency("aws-java-sdk-bundle", "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/" + AWS_JAVA_SDK_VERSION + "/aws-java-sdk-bundle-" + AWS_JAVA_SDK_VERSION + ".jar");
    private static final JarDependency HADOOP_AWS_JAR = new JarDependency("hadoop-aws", "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/" + HADOOP_AWS_VERSION + "/hadoop-aws-" + HADOOP_AWS_VERSION + ".jar");
    private static final JarDependency REDSHIFT_JDBC_JAR = new JarDependency("redshift-jdbc42", "https://repo1.maven.org/maven2/com/amazon/redshift/redshift-jdbc42/" + REDSHIFT_JDBC_VERSION + "/redshift-jdbc42-" + REDSHIFT_JDBC_VERSION + ".jar");
    private static final JarDependency SPARK_REDSHIFT_JAR = new JarDependency("spark-redshift", "https://repo1.maven.org/maven2/io/github/spark-redshift-community/spark-redshift_" + SCALA_VERSION +
            "/" + SPARK_REDSHIFT_VERSION + "/spark-redshift_" + SCALA_VERSION + "-" + SPARK_REDSHIFT_VERSION + ".jar");
    private static final JarDependency STARLAKE_SNAPSHOT_JAR = new JarDependency("starlake-spark", "https://s01.oss.sonatype.org/content/repositories/snapshots/ai/starlake/starlake-spark" + SPARK_MAJOR_VERSION + "_" + SCALA_VERSION + "/" + SL_VERSION + "/starlake-spark" + SPARK_MAJOR_VERSION + "_" + SCALA_VERSION + "-" + SL_VERSION + "-assembly.jar");
    private static final JarDependency STARLAKE_RELEASE_JAR = new JarDependency("starlake-spark", "https://s01.oss.sonatype.org/content/repositories/releases/ai/starlake/starlake-spark" + SPARK_MAJOR_VERSION + "_" + SCALA_VERSION + "/" + SL_VERSION + "/starlake-spark" + SPARK_MAJOR_VERSION + "_" + SCALA_VERSION + "-" + SL_VERSION + "-assembly.jar");

    private static final JarDependency[] snowflakeDependencies = {
            SNOWFLAKE_JDBC_JAR,
            SPARK_SNOWFLAKE_JAR
    };

    private static final JarDependency[] redshiftDependencies = {
            AWS_JAVA_SDK_JAR,
            HADOOP_AWS_JAR,
            REDSHIFT_JDBC_JAR,
            SPARK_REDSHIFT_JAR
    };

    private static final JarDependency[] azureDependencies = {
            HADOOP_AZURE_JAR,
            AZURE_STORAGE_JAR,
            JETTY_SERVER_JAR
    };

    private static final JarDependency[] postgresqlDependencies = {
            POSTGRESQL_JAR
    };

    private static final JarDependency[] bigqueryDependencies = {
            SPARK_BQ_JAR
    };

    private static Optional<String> getEnv(String env) {
        // consider empty env variables as not set
        return Optional.ofNullable(System.getenv(env)).filter(s -> !s.isEmpty());
    }

    private static boolean envIsTrue(String env) {
        String value = getEnv(env).orElse("false");
        return !value.equals("false") && !value.equals("0");

    }

    private static void generateUnixVersions(File targetDir) throws IOException {
        generateVersions(targetDir, "#!/bin/bash\nset -e\n\n",
                (writer) -> (variableName, value) -> {
                    try {
                        writer.write(variableName + "=" + "${" + variableName + ":-" + value + "}\n");
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private static void generateWindowsVersions(File targetDir) throws IOException {
        generateVersions(targetDir, "@ECHO OFF\n\n",
                (writer) -> (variableName, value) -> {
                    try {
                        writer.write(
                                "if \"%" + variableName + "%\"==\"\" (\n" +
                                        "    SET " + variableName + "=" + value + "\n" +
                                        ")\n");
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    // Used BiConsumer with Function because TriConsumer doesn't exist natively and avoid creating a new type
    private static void generateVersions(File targetDir, String fileHeader, Function<BufferedWriter, BiConsumer<String, String>> variableWriter) throws IOException {
        String versionsFileName = isWindowsOs() ? "versions.cmd" : "versions.sh";
        File versionFile = new File(targetDir, versionsFileName);
        deleteFile(versionFile);
        BufferedWriter writer = new BufferedWriter(new FileWriter(versionFile));
        try {
            writer.write(fileHeader);
            variableWriter.apply(writer).accept("ENABLE_BIGQUERY", String.valueOf(ENABLE_BIGQUERY));
            variableWriter.apply(writer).accept("ENABLE_AZURE", String.valueOf(ENABLE_AZURE));
            variableWriter.apply(writer).accept("ENABLE_SNOWFLAKE", String.valueOf(ENABLE_SNOWFLAKE));
            variableWriter.apply(writer).accept("ENABLE_POSTGRESQL", String.valueOf(ENABLE_POSTGRESQL));
            variableWriter.apply(writer).accept("ENABLE_REDSHIFT", String.valueOf(ENABLE_REDSHIFT));
            variableWriter.apply(writer).accept("SL_VERSION", SL_VERSION);
            variableWriter.apply(writer).accept("SCALA_VERSION", SCALA_VERSION);
            variableWriter.apply(writer).accept("SPARK_VERSION", SPARK_VERSION);
            variableWriter.apply(writer).accept("HADOOP_VERSION", HADOOP_VERSION);
            if (ENABLE_BIGQUERY || !anyDependencyEnabled()) {
                variableWriter.apply(writer).accept("SPARK_BQ_VERSION", SPARK_BQ_VERSION);
            }
            if (ENABLE_AZURE || !anyDependencyEnabled()) {
                variableWriter.apply(writer).accept("HADOOP_AZURE_VERSION", HADOOP_AZURE_VERSION);
                variableWriter.apply(writer).accept("AZURE_STORAGE_VERSION", AZURE_STORAGE_VERSION);
                variableWriter.apply(writer).accept("JETTY_VERSION", JETTY_VERSION);
            }
            if (ENABLE_SNOWFLAKE || !anyDependencyEnabled()) {
                variableWriter.apply(writer).accept("SPARK_SNOWFLAKE_VERSION", SPARK_SNOWFLAKE_VERSION);
                variableWriter.apply(writer).accept("SNOWFLAKE_JDBC_VERSION", SNOWFLAKE_JDBC_VERSION);
            }
            if (ENABLE_POSTGRESQL || !anyDependencyEnabled()) {
                variableWriter.apply(writer).accept("POSTGRESQL_VERSION", POSTGRESQL_VERSION);
            }
            if (ENABLE_REDSHIFT || !anyDependencyEnabled()) {
                variableWriter.apply(writer).accept("AWS_JAVA_SDK_VERSION", AWS_JAVA_SDK_VERSION);
                variableWriter.apply(writer).accept("HADOOP_AWS_VERSION", HADOOP_AWS_VERSION);
                variableWriter.apply(writer).accept("REDSHIFT_JDBC_VERSION", REDSHIFT_JDBC_VERSION);
                variableWriter.apply(writer).accept("SPARK_REDSHIFT_VERSION", SPARK_REDSHIFT_VERSION);
            }
        } finally {
            writer.close();
        }
        System.out.println(versionFile.getAbsolutePath() + " created");
    }

    private static void generateVersions(File targetDir) throws IOException {
        if (isWindowsOs()) {
            generateWindowsVersions(targetDir);
        } else {
            generateUnixVersions(targetDir);
        }

    }

    private static boolean anyDependencyEnabled() {
        return ENABLE_BIGQUERY || ENABLE_AZURE || ENABLE_SNOWFLAKE || ENABLE_REDSHIFT || ENABLE_POSTGRESQL;
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


            if (!anyDependencyEnabled()) {
                ENABLE_AZURE = true;
                ENABLE_BIGQUERY = true;
                ENABLE_SNOWFLAKE = true;
                ENABLE_REDSHIFT = true;
                ENABLE_POSTGRESQL = true;
            }
            final File binDir = new File(targetDir, "bin");

            File slDir = new File(binDir, "sl");
            if (SL_VERSION.endsWith("SNAPSHOT")) {
                deleteFile(new File(slDir, STARLAKE_SNAPSHOT_JAR.getUrlName()));
                downloadAndDisplayProgress(new JarDependency[]{STARLAKE_SNAPSHOT_JAR}, slDir, false);
            } else {
                deleteFile(new File(slDir, STARLAKE_RELEASE_JAR.getUrlName()));
                downloadAndDisplayProgress(new JarDependency[]{STARLAKE_RELEASE_JAR}, slDir, false);
            }

            File sparkDir = new File(binDir, "spark");
            if (!sparkDir.exists()) {
                downloadSpark(binDir);
            }

            File depsDir = new File(binDir, "deps");

            if (ENABLE_BIGQUERY) {
                downloadAndDisplayProgress(bigqueryDependencies, depsDir, true);
            } else {
                deleteDependencies(bigqueryDependencies, depsDir);
            }
            if (ENABLE_AZURE) {
                downloadAndDisplayProgress(azureDependencies, depsDir, true);
            } else {
                deleteDependencies(azureDependencies, depsDir);
            }
            if (ENABLE_SNOWFLAKE) {
                downloadAndDisplayProgress(snowflakeDependencies, depsDir, true);
            } else {
                deleteDependencies(snowflakeDependencies, depsDir);
            }
            if (ENABLE_REDSHIFT) {
                downloadAndDisplayProgress(redshiftDependencies, depsDir, true);
            } else {
                deleteDependencies(redshiftDependencies, depsDir);
            }
            if (ENABLE_POSTGRESQL) {
                downloadAndDisplayProgress(postgresqlDependencies, depsDir, true);
            } else {
                deleteDependencies(postgresqlDependencies, depsDir);
            }
            generateVersions(targetDir);
        } catch (Exception e) {
            System.out.println("Failed to download dependencies from maven central" + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void downloadSpark(File binDir) throws IOException {
        downloadAndDisplayProgress(new JarDependency[]{SPARK_JAR}, binDir, false);
        String tgzName = SPARK_JAR.getUrlName();
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

    private static void downloadAndDisplayProgress(JarDependency[] dependencies, File targetDir, boolean replaceJar) throws IOException {
        if (!targetDir.exists()) {
            targetDir.mkdirs();
        }
        if (replaceJar) {
            deleteDependencies(dependencies, targetDir);
        }
        for (JarDependency dependency : dependencies) {
            final File targetFile = new File(targetDir, dependency.getUrlName());
            downloadAndDisplayProgress(dependency.url, targetFile.getAbsolutePath());
        }
    }

    private static void deleteDependencies(JarDependency[] dependencies, File targetDir) {
        if (targetDir.exists()) {
            for (JarDependency dependency : dependencies) {
                File[] files = targetDir.listFiles(f -> f.getName().startsWith(dependency.artefactName));
                if (files != null) {
                    for (File file : files) {
                        deleteFile(file);
                    }
                }
            }
        }
    }

    private static void deleteFile(File file) {
        if (file.exists()) {
            if (file.delete()) {
                System.out.println(file.getAbsolutePath() + " deleted");
            }
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
        InputStream input = new BufferedInputStream(url.openStream());
        OutputStream output = new FileOutputStream(file);
        byte data[] = new byte[CHUNK_SIZE];
        long total = 0;
        int count;
        int loop = 0;
        int sbLen = 0;
        long lastTime = System.currentTimeMillis();
        while ((count = input.read(data)) != -1) {
            total += count;
            output.write(data, 0, count);
            loop++;
            if (loop % 1000 == 0) {
                StringBuilder sb = new StringBuilder("Progress: " + (total / 1024 / 1024) + "/" + (lengthOfFile / 1024 / 1024) + " MB");
                if (lengthOfFile > 0) {
                    sb.append(" (");
                    sb.append((int) (total * 100 / lengthOfFile));
                    sb.append("%)");
                }
                long currentTime = System.currentTimeMillis();
                long timeDiff = currentTime - lastTime;
                double bytesPerMilliSec = (CHUNK_SIZE * 1000.0 / timeDiff);
                double bytesPerSec = bytesPerMilliSec * 1000;
                double mbPerSec = bytesPerSec / 1024 / 1024;
                sb.append(" ");
                sb.append(String.format("[%.2f MB/sec]", mbPerSec));
                lastTime = currentTime;
                sbLen = sb.length();
                for (int cnt = 0; cnt < sbLen; cnt++) {
                    System.out.print("\b");
                }
                System.out.print(sb);
            }
        }
        for (int cnt = 0; cnt < sbLen; cnt++) {
            System.out.print("\b");
        }
        System.out.print(name + " downloaded");
        System.out.println();
        output.flush();
        output.close();
        input.close();
    }
}
