import javax.net.ssl.*;
import java.io.*;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Setup extends ProxySelector implements X509TrustManager {

    private static class  UserPwdAuth extends  Authenticator {
        @Override
        protected PasswordAuthentication getPasswordAuthentication() {
            return new PasswordAuthentication(username, password.toCharArray());
        }
    };

    private static class ResourceDependency {

        private final String[] urls;

        private final String artefactName;

        public ResourceDependency(String artefactName, String... url) {
            this.urls = url;
            this.artefactName = artefactName;
        }

        public List<String> getUrlNames() {
            return Arrays.stream(urls).map(this::getUrlName).collect(Collectors.toList());
        }

        public String getUrlName(String url) {
            return url.substring(url.lastIndexOf("/") + 1);
        }
    }

    private static String protocol = null;
    private static String host = null;
    private static int port = 0;
    private static String username = null;
    private static String password = null;

    private static String httpsProxy = getEnv("https_proxy").orElse("");
    private static String httpProxy = getEnv("http_proxy").orElse("");
    private static String noProxy = getEnv("no_proxy").orElse("").replaceAll(",", "|");

    private static Proxy proxy = Proxy.NO_PROXY;
    private static HttpClient client = null;

    private static boolean isWindowsOs() {
        String os = System.getProperty("os.name").toLowerCase();
        return os.startsWith("windows");
    }

    private static void parseProxy(String proxy) {
        if (proxy.isEmpty()) {
            return;
        }
        final Pattern pattern = Pattern.compile("(https?|socks5?):\\/\\/([^:].+)", Pattern.CASE_INSENSITIVE);
        final Matcher m = pattern.matcher(proxy);
        if (m.matches()) {
            protocol = m.group(1).toLowerCase();
            final String hostAndPortWithMaybeCredentials = m.group(2);
            if (hostAndPortWithMaybeCredentials.contains("@")) {
                final String[] hostAndPortWithCredentials = hostAndPortWithMaybeCredentials.split("@");
                final String[] credentials = hostAndPortWithCredentials[0].split(":");
                assert(credentials.length == 2): "Invalid credentials format, expecting 'username:password'";
                username = credentials[0];
                password = credentials[1];
                final String[] hostAndPort = hostAndPortWithCredentials[1].split(":");
                host = hostAndPort[0];
                if (hostAndPort.length > 1) {
                    port = Integer.parseInt(hostAndPort[1]);
                }
            } else {
                final String[] hostAndPort = hostAndPortWithMaybeCredentials.split(":");
                host = hostAndPort[0];
                if (hostAndPort.length > 1) {
                    port = Integer.parseInt(hostAndPort[1]);
                }
            }
        } else {
            throw new IllegalArgumentException("Invalid proxy format: " + proxy);
        }
    }

    private static void setProxy() {
        if (!httpsProxy.isEmpty()) {
            parseProxy(httpsProxy);
        } else if (!httpProxy.isEmpty()) {
            parseProxy(httpProxy);
        }
        if (host != null) {
            if (port == 0) {
                if (protocol.equals("https")) {
                    port = 443;
                } else if (protocol.startsWith("socks")) {
                    port = 1080;
                } else {
                    port = 80;
                }
            }
            Proxy.Type proxyType = Proxy.Type.HTTP;
            if (protocol.startsWith("socks")) {
                proxyType = Proxy.Type.SOCKS;
            }
            proxy = new Proxy(proxyType, new InetSocketAddress(host, port));
        }
        if (!noProxy.isEmpty()) {
            System.setProperty("http.nonProxyHosts", noProxy);
        }
    }

    // ENV VARS
    public static boolean ENABLE_ALL = envIsTrue("ENABLE_ALL");
    public static boolean ENABLE_BIGQUERY = ENABLE_ALL || envIsTrue("ENABLE_BIGQUERY");
    public static boolean ENABLE_AZURE = ENABLE_ALL || envIsTrue("ENABLE_AZURE");
    public static boolean ENABLE_SNOWFLAKE = ENABLE_ALL || envIsTrue("ENABLE_SNOWFLAKE");
    public static boolean ENABLE_REDSHIFT = ENABLE_ALL || envIsTrue("ENABLE_REDSHIFT");
    public static boolean ENABLE_POSTGRESQL = ENABLE_ALL || envIsTrue("ENABLE_POSTGRESQL");
    public static boolean ENABLE_DUCKDB = ENABLE_ALL || envIsTrue("ENABLE_DUCKDB");
    public static boolean ENABLE_KAFKA = ENABLE_ALL || envIsTrue("ENABLE_KAFKA");

    private static final boolean[] ALL_ENABLERS = new boolean[] {
            ENABLE_BIGQUERY,
            ENABLE_AZURE,
            ENABLE_SNOWFLAKE,
            ENABLE_REDSHIFT,
            ENABLE_POSTGRESQL,
            ENABLE_DUCKDB,
            ENABLE_KAFKA
    };

    // SCALA 2.12 by default until spark redshift is available for 2.13
    private static final String SCALA_VERSION = getEnv("SCALA_VERSION").orElse("2.13");

    // STARLAKE
    private static final String SL_VERSION = getEnv("SL_VERSION").orElse("1.3.0");

    // SPARK
    private static final String SPARK_VERSION = getEnv("SPARK_VERSION").orElse("3.5.3");
    private static final String HADOOP_VERSION = getEnv("HADOOP_VERSION").orElse("3");


    // BIGQUERY
    private static final String SPARK_BQ_VERSION = getEnv("SPARK_BQ_VERSION").orElse("0.41.1");

    // deltalake
    private static final String DELTA_SPARK = getEnv("SPARK_DELTA").orElse("3.3.0");

    private static final String HADOOP_AZURE_VERSION = getEnv("HADOOP_AZURE_VERSION").orElse("3.3.5");
    private static final String AZURE_STORAGE_VERSION = getEnv("AZURE_STORAGE_VERSION").orElse("8.6.6");
    private static final String JETTY_VERSION = getEnv("JETTY_VERSION").orElse("9.4.51.v20230217");

    // HADOOP_LIB ON WINDOWS
    private static final ResourceDependency[] HADOOP_LIBS = new ResourceDependency[]{
            new ResourceDependency("winutils", "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/winutils.exe"),
            new ResourceDependency("hadoop.dll", "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/hadoop.dll")
    };

    // SNOWFLAKE
    private static final String SNOWFLAKE_JDBC_VERSION = getEnv("SNOWFLAKE_JDBC_VERSION").orElse("3.21.0");
    private static final String SPARK_SNOWFLAKE_VERSION = getEnv("SPARK_SNOWFLAKE_VERSION").orElse("3.1.1");

    // POSTGRESQL
    private static final String POSTGRESQL_VERSION = getEnv("POSTGRESQL_VERSION").orElse("42.5.4");

    // DUCKDB
    private static final String DUCKDB_VERSION = getEnv("DUCKDB_VERSION").orElse("1.1.3");

    // REDSHIFT
    private static final String AWS_JAVA_SDK_VERSION = getEnv("AWS_JAVA_SDK_VERSION").orElse("1.12.780");
    private static final String HADOOP_AWS_VERSION = getEnv("HADOOP_AWS_VERSION").orElse("3.3.4");
    private static final String REDSHIFT_JDBC_VERSION = getEnv("REDSHIFT_JDBC_VERSION").orElse("2.1.0.32");
    private static  String SPARK_REDSHIFT_VERSION() {
        if (SCALA_VERSION.equals("2.13")) {
            return getEnv("SPARK_REDSHIFT_VERSION").orElse("6.3.0-spark_3.5-SNAPSHOT");
        } else {
            return getEnv("SPARK_REDSHIFT_VERSION").orElse("6.3.0-spark_3.5");
        }
    }

    private static final String CONFLUENT_VERSION = getEnv("CONFLUENT_VERSION").orElse("7.7.2");

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    // DUCKDB
    private static final ResourceDependency SPARK_JAR = new ResourceDependency("spark", "https://archive.apache.org/dist/spark/spark-" + SPARK_VERSION + "/spark-" + SPARK_VERSION + "-bin-hadoop" + HADOOP_VERSION + ".tgz");
    private static final ResourceDependency SPARK_JAR_213 = new ResourceDependency("spark", "https://archive.apache.org/dist/spark/spark-" + SPARK_VERSION + "/spark-" + SPARK_VERSION + "-bin-hadoop" + HADOOP_VERSION + "-scala2.13.tgz");
    private static final ResourceDependency SPARK_BQ_JAR = new ResourceDependency("spark-bigquery-with-dependencies",
            "https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_" + SCALA_VERSION + "/" +
                    SPARK_BQ_VERSION + "/" +
                    "spark-bigquery-with-dependencies_" + SCALA_VERSION + "-" + SPARK_BQ_VERSION + ".jar");
    private static final ResourceDependency DELTA_SPARK_JAR = new ResourceDependency("delta-spark",
            "https://repo1.maven.org/maven2/io/delta/delta-spark_" + SCALA_VERSION + "/" + DELTA_SPARK + "/delta-spark_" + SCALA_VERSION + "-" + DELTA_SPARK + ".jar");

    private static final ResourceDependency DELTA_STORAGE_JAR = new ResourceDependency("delta-storage",
            "https://repo1.maven.org/maven2/io/delta/delta-storage" + "/" + DELTA_SPARK + "/delta-storage" +"-" + DELTA_SPARK + ".jar");
    private static final ResourceDependency HADOOP_AZURE_JAR = new ResourceDependency("hadoop-azure", "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/" + HADOOP_AZURE_VERSION + "/hadoop-azure-" + HADOOP_AZURE_VERSION + ".jar");
    private static final ResourceDependency AZURE_STORAGE_JAR = new ResourceDependency("azure-storage", "https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/" + AZURE_STORAGE_VERSION + "/azure-storage-" + AZURE_STORAGE_VERSION + ".jar");
    private static final ResourceDependency JETTY_SERVER_JAR = new ResourceDependency("jetty-server", "https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-server/" + JETTY_VERSION + "/jetty-server-" + JETTY_VERSION + ".jar");
    private static final ResourceDependency SNOWFLAKE_JDBC_JAR = new ResourceDependency("snowflake-jdbc", "https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/" + SNOWFLAKE_JDBC_VERSION + "/snowflake-jdbc-" + SNOWFLAKE_JDBC_VERSION + ".jar");
    private static final ResourceDependency SPARK_SNOWFLAKE_JAR = new ResourceDependency("spark-snowflake", "https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_" + SCALA_VERSION +
            "/" + SPARK_SNOWFLAKE_VERSION + "/spark-snowflake_" + SCALA_VERSION + "-" + SPARK_SNOWFLAKE_VERSION + ".jar");
    private static final ResourceDependency POSTGRESQL_JAR = new ResourceDependency("postgresql", "https://repo1.maven.org/maven2/org/postgresql/postgresql/" + POSTGRESQL_VERSION + "/postgresql-" + POSTGRESQL_VERSION + ".jar");

    private static final ResourceDependency DUCKDB_JAR = new ResourceDependency("duckdb_jdbc", "https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/" + DUCKDB_VERSION + "/duckdb_jdbc-" + DUCKDB_VERSION + ".jar");
    private static final ResourceDependency AWS_JAVA_SDK_JAR = new ResourceDependency("aws-java-sdk-bundle", "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/" + AWS_JAVA_SDK_VERSION + "/aws-java-sdk-bundle-" + AWS_JAVA_SDK_VERSION + ".jar");
    private static final ResourceDependency HADOOP_AWS_JAR = new ResourceDependency("hadoop-aws", "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/" + HADOOP_AWS_VERSION + "/hadoop-aws-" + HADOOP_AWS_VERSION + ".jar");
    private static final ResourceDependency REDSHIFT_JDBC_JAR = new ResourceDependency("redshift-jdbc42", "https://repo1.maven.org/maven2/com/amazon/redshift/redshift-jdbc42/" + REDSHIFT_JDBC_VERSION + "/redshift-jdbc42-" + REDSHIFT_JDBC_VERSION + ".jar");
    private static ResourceDependency SPARK_REDSHIFT_JAR() {
        if (SCALA_VERSION.equals("2.13")) {
            return new ResourceDependency("spark-redshift", "https://s01.oss.sonatype.org/content/repositories/snapshots/ai/starlake/spark-redshift_" + SCALA_VERSION +
                    "/" + SPARK_REDSHIFT_VERSION() + "/spark-redshift_" + SCALA_VERSION + "-" + SPARK_REDSHIFT_VERSION() + ".jar");
        }
        else {
        return new ResourceDependency("spark-redshift", "https://repo1.maven.org/maven2/io/github/spark-redshift-community/spark-redshift_" + SCALA_VERSION +
            "/" + SPARK_REDSHIFT_VERSION() + "/spark-redshift_" + SCALA_VERSION + "-" + SPARK_REDSHIFT_VERSION() + ".jar");
        }
    }
    private static final ResourceDependency STARLAKE_SNAPSHOT_JAR = new ResourceDependency("starlake-core", "https://s01.oss.sonatype.org/content/repositories/snapshots/ai/starlake/starlake-core" + "_" + SCALA_VERSION + "/" + SL_VERSION + "/starlake-core"+ "_" + SCALA_VERSION + "-" + SL_VERSION + "-assembly.jar");
    private static final ResourceDependency STARLAKE_RELEASE_JAR = new ResourceDependency("starlake-core", "https://s01.oss.sonatype.org/content/repositories/releases/ai/starlake/starlake-core" + "_" + SCALA_VERSION + "/" + SL_VERSION + "/starlake-core" + "_" + SCALA_VERSION + "-" + SL_VERSION + "-assembly.jar", "https://s01.oss.sonatype.org/content/repositories/releases/ai/starlake/starlake-spark3" + "_" + SCALA_VERSION + "/" + SL_VERSION + "/starlake-spark3" + "_" + SCALA_VERSION + "-" + SL_VERSION + "-assembly.jar");
    private static final ResourceDependency CONFLUENT_KAFKA_SCHEMA_REGISTRY_CLIENT = new ResourceDependency("kafka-schema-registry-client", "https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/" + CONFLUENT_VERSION + "/kafka-schema-registry-client-" + CONFLUENT_VERSION + ".jar");
    private static final ResourceDependency CONFLUENT_KAFKA_AVRO_SERIALIZER = new ResourceDependency("kafka-avro-serializer", "https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/" + CONFLUENT_VERSION + "/kafka-avro-serializer-" + CONFLUENT_VERSION + ".jar");

    private static final ResourceDependency[] snowflakeDependencies = {
            SNOWFLAKE_JDBC_JAR,
            SPARK_SNOWFLAKE_JAR
    };

    private static final ResourceDependency[] redshiftDependencies = {
            AWS_JAVA_SDK_JAR,
            HADOOP_AWS_JAR,
            REDSHIFT_JDBC_JAR,
            SPARK_REDSHIFT_JAR()
    };

    private static final ResourceDependency[] azureDependencies = {
            HADOOP_AZURE_JAR,
            AZURE_STORAGE_JAR,
            JETTY_SERVER_JAR
    };

    private static final ResourceDependency[] postgresqlDependencies = {
            POSTGRESQL_JAR
    };

    private static final ResourceDependency[] duckDbDependencies = {
            DUCKDB_JAR
    };

    private static final ResourceDependency[] bigqueryDependencies = {
            SPARK_BQ_JAR
    };

    private static final ResourceDependency[] sparkDependencies = {
            DELTA_SPARK_JAR,
            DELTA_STORAGE_JAR
    };

    private static final ResourceDependency[] confluentDependencies = {
            CONFLUENT_KAFKA_SCHEMA_REGISTRY_CLIENT,
            CONFLUENT_KAFKA_AVRO_SERIALIZER
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
        generateVersions(targetDir, "versions.sh", "#!/bin/bash\nset -e\n\n",
                (writer) -> (variableName, value) -> {
                    try {
                        writer.write(variableName + "=" + "${" + variableName + ":-" + value + "}\n");
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private static void generateWindowsVersions(File targetDir) throws IOException {
        generateVersions(targetDir, "versions.cmd", "@ECHO OFF\n\n",
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
    private static void generateVersions(File targetDir, String versionsFileName, String fileHeader, Function<BufferedWriter, BiConsumer<String, String>> variableWriter) throws IOException {
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
            variableWriter.apply(writer).accept("ENABLE_KAFKA", String.valueOf(ENABLE_KAFKA));
            variableWriter.apply(writer).accept("SL_VERSION", SL_VERSION);
            variableWriter.apply(writer).accept("SCALA_VERSION", SCALA_VERSION);
            variableWriter.apply(writer).accept("SPARK_VERSION", SPARK_VERSION);
            variableWriter.apply(writer).accept("HADOOP_VERSION", HADOOP_VERSION);
            variableWriter.apply(writer).accept("DUCKDB_VERSION", DUCKDB_VERSION);

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
                variableWriter.apply(writer).accept("SPARK_REDSHIFT_VERSION", SPARK_REDSHIFT_VERSION());
            }
            if (ENABLE_KAFKA || !anyDependencyEnabled()) {
                variableWriter.apply(writer).accept("CONFLUENT_VERSION", CONFLUENT_VERSION);
            }
        } finally {
            writer.close();
        }
        System.out.println(versionFile.getAbsolutePath() + " created");
    }

    private static void generateVersions(File targetDir, boolean unix) throws IOException {
        if (isWindowsOs() && !unix) {
            generateWindowsVersions(targetDir);
        } else {
            generateUnixVersions(targetDir);
        }

    }

    private static boolean anyDependencyEnabled() {
        for (boolean enabled : ALL_ENABLERS) {
            if (enabled) {
                return true;
            }
        }
        return ENABLE_ALL;
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
    }

    @Override
    public List<Proxy> select(URI uri) {
        return Collections.singletonList(proxy);
    }

    @Override
    public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {
        throw new RuntimeException("Failed to connect to " + uri + " using proxy " + sa);
    }

    private static final Setup instance = new Setup();

    private static final TrustManager alwaysTrustManager = instance;

    private static final ProxySelector proxySelector = instance;

    private static void setHttpClient() throws NoSuchAlgorithmException, KeyManagementException {
        setProxy();
        HttpClient.Builder clientBuilder = HttpClient.newBuilder();
        clientBuilder.proxy(proxySelector);
        if (username != null && password != null) {
            Authenticator authenticator = new UserPwdAuth();
            clientBuilder.authenticator(authenticator);
        }
        if (host != null && envIsTrue("SL_INSECURE")) {
            System.out.println("Enabling insecure mode for SSL connections using proxy " + protocol + "://" + host + ":" + port);
            // Create a trust manager that does not validate certificate chains
            TrustManager[] trustAllCerts = new TrustManager[]{alwaysTrustManager};

            // Install the all-trusting trust manager
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            clientBuilder.sslContext(sc);
        }
        client = clientBuilder.build();
    }

    private static void updateSparkLog4j2Properties(File sparkDir) {
        File log4jFile = new File(new File(sparkDir, "conf"), "log4j2.properties");
        try {
            BufferedReader reader = new BufferedReader(new FileReader(log4jFile));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("rootLogger.level =")|| line.startsWith("rootLogger.level=")) {
                    line = "rootLogger.level = ${env:SL_LOG_LEVEL:-error}";
                }
                sb.append(line).append("\n");
            }
            reader.close();

            sb.append("logger.shutdown.name=org.apache.spark.util.ShutdownHookManager").append("\n");
            sb.append("logger.shutdown.level=OFF").append("\n");
            sb.append("logger.env.name=org.apache.spark.SparkEnv").append("\n");
            sb.append("logger.env.level=error").append("\n");

            BufferedWriter writer = new BufferedWriter(new FileWriter(log4jFile));
            writer.write(sb.toString());
            writer.close();
        } catch (IOException e) {
            System.out.println("Failed to update log4j.properties");
            e.printStackTrace();
        }
    }

    private static void askUserWhichConfigToEnable() {
        if (!anyDependencyEnabled()) {
            System.out.print("Do you want to enable all datawarehouse configurations [y/n] ? ");
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                String answer = reader.readLine();
                if (answer.equalsIgnoreCase("y")) {
                    ENABLE_AZURE = true;
                    ENABLE_BIGQUERY = true;
                    ENABLE_SNOWFLAKE = true;
                    ENABLE_REDSHIFT = true;
                    ENABLE_POSTGRESQL = true;
                    ENABLE_DUCKDB = true;
                    ENABLE_KAFKA = true;
                } else {
                    System.out.println("Please enable the configurations you want to use by setting the corresponding environment variables below");
                    System.out.println("ENABLE_BIGQUERY, ENABLE_DATABRICKS, ENABLE_AZURE, ENABLE_SNOWFLAKE, ENABLE_REDSHIFT, ENABLE_POSTGRESQL, ENABLE_ANY_JDBC, ENABLE_KAFKA");
                    System.exit(1);
                }
            } catch (IOException e) {
                System.out.println("Failed to read user input");
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args) throws IOException {
        try {
            if (args.length == 0) {
                System.out.println("Please specify the target directory");
                System.exit(1);
            }

            askUserWhichConfigToEnable();

            final File targetDir = new File(args[0]);
            if (!targetDir.exists()) {
                targetDir.mkdirs();
                System.out.println("Created target directory " + targetDir.getAbsolutePath());
            }

            setHttpClient();

            if (!anyDependencyEnabled()) {
                ENABLE_AZURE = true;
                ENABLE_BIGQUERY = true;
                ENABLE_SNOWFLAKE = true;
                ENABLE_REDSHIFT = true;
                ENABLE_POSTGRESQL = true;
                ENABLE_DUCKDB = true;
                ENABLE_KAFKA = true;
            }
            final File binDir = new File(targetDir, "bin");

            if (isWindowsOs()) {
                final File hadoopDir = new File(binDir, "hadoop");
                final File hadoopBinDir = new File(hadoopDir, "bin");
                if (!hadoopBinDir.exists()) {
                    hadoopBinDir.mkdirs();
                }
                for (ResourceDependency lib : HADOOP_LIBS) {
                    downloadAndDisplayProgress(lib, (resource, url) -> new File(hadoopBinDir, resource.getUrlName(url)));
                }

            } else {
                System.out.println("Unix OS detected");
            }

            File slDir = new File(binDir, "sl");
            if (SL_VERSION.endsWith("SNAPSHOT")) {
                STARLAKE_SNAPSHOT_JAR.getUrlNames().forEach(urlName -> deleteFile(new File(slDir, urlName)));
                downloadAndDisplayProgress(new ResourceDependency[]{STARLAKE_SNAPSHOT_JAR}, slDir, false);
            } else {
                STARLAKE_RELEASE_JAR.getUrlNames().forEach(urlName -> deleteFile(new File(slDir, urlName)));
                downloadAndDisplayProgress(new ResourceDependency[]{STARLAKE_RELEASE_JAR}, slDir, false);
            }

            File sparkDir = new File(binDir, "spark");
            if (!sparkDir.exists()) {
                downloadSpark(binDir);
            }

            File depsDir = new File(binDir, "deps");

            downloadAndDisplayProgress(sparkDependencies, depsDir, true);
            updateSparkLog4j2Properties(sparkDir);
            downloadAndDisplayProgress(duckDbDependencies, depsDir, true);

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
            if (ENABLE_KAFKA) {
                downloadAndDisplayProgress(confluentDependencies, depsDir, true);
            } else {
                deleteDependencies(confluentDependencies, depsDir);
            }

            boolean unix = args.length > 1 && args[1].equalsIgnoreCase("unix");
            generateVersions(targetDir, unix);
        } catch (Exception e) {
            System.out.println("Failed to download dependency: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void downloadSpark(File binDir) throws IOException, InterruptedException {
        ResourceDependency sparkJar = SPARK_JAR;
        if (!SCALA_VERSION.equals("2.12")) {
            sparkJar = SPARK_JAR_213;
        }
        downloadAndDisplayProgress(new ResourceDependency[]{sparkJar}, binDir, false);
        sparkJar.getUrlNames().stream().map(tgzName -> new File(binDir, tgzName)).filter(File::exists).forEach(sparkFile -> {
            String tgzName = sparkFile.getName();
            ProcessBuilder builder = new ProcessBuilder("tar", "-xzf", sparkFile.getAbsolutePath(), "-C", binDir.getAbsolutePath()).inheritIO();
            try {
                Process process = builder.start();
                process.waitFor();
            } catch (InterruptedException | IOException e) {
                System.out.println("Failed to extract spark tarball");
                e.printStackTrace();
            }
            sparkFile.delete();
            File sparkDir = new File(binDir, tgzName.substring(0, tgzName.lastIndexOf(".")));
            sparkDir.renameTo(new File(binDir, "spark"));
            sparkDir = new File(binDir, "spark");
            File log4j2File = new File(sparkDir, "conf/log4j2.properties.template");
            log4j2File.renameTo(new File(sparkDir, "conf/log4j2.properties"));
        });
    }

    private static void downloadAndDisplayProgress(ResourceDependency[] dependencies, File targetDir, boolean replaceJar) throws IOException, InterruptedException {
        if (!targetDir.exists()) {
            targetDir.mkdirs();
        }
        if (replaceJar) {
            deleteDependencies(dependencies, targetDir);
        }
        for (ResourceDependency dependency : dependencies) {
            downloadAndDisplayProgress(dependency, (resource, url) -> new File(targetDir, resource.getUrlName(url)));
        }
    }

    private static void deleteDependencies(ResourceDependency[] dependencies, File targetDir) {
        if (targetDir.exists()) {
            for (ResourceDependency dependency : dependencies) {
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

    private static void downloadAndDisplayProgress(ResourceDependency resource, BiFunction<ResourceDependency, String, File> fileProducer) throws IOException, InterruptedException {
        boolean succesfullyDownloaded = false;
        List<String> triedUrlList = new ArrayList<>();
        System.out.println("Downloading " + resource.artefactName + "...");
        for (String urlStr : resource.urls) {
            File file = fileProducer.apply(resource, urlStr);
            final int CHUNK_SIZE = 1024;
            int filePartIndex = urlStr.lastIndexOf("/") + 1;
            String name = urlStr.substring(filePartIndex);
            String urlFolder = urlStr.substring(0, filePartIndex);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlStr))
                    .build();
            HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
            if (response.statusCode() == 200) {
                long lengthOfFile = response.headers().firstValueAsLong("Content-Length").orElse(0L);
                InputStream input = new BufferedInputStream(response.body());
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
                        StringBuilder sb = new StringBuilder(" " + (total / 1024 / 1024) + "/" + (lengthOfFile / 1024 / 1024) + " MB");
                        if (lengthOfFile > 0) {
                            sb.append(" (");
                            sb.append(total * 100 / lengthOfFile);
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
                System.out.print(file.getAbsolutePath() + " succesfully downloaded from " + urlFolder);
                System.out.println();
                output.flush();
                output.close();
                input.close();
                succesfullyDownloaded = true;
                break;
            } else {
                triedUrlList.add(urlStr + " (" + response.statusCode() + ")");
            }
        }
        if (!succesfullyDownloaded) {
            String triedUrls = String.join(" and ", triedUrlList);
            throw new RuntimeException("Failed to fetch " + resource.artefactName + " from " + triedUrls);
        }
    }
}
