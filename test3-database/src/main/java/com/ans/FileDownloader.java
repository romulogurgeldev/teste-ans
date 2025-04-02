package com.ans;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class FileDownloader {
    private static final String ANS_BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/";
    private static final int TIMEOUT = 120000; // 2 minutos

    // Padrões para identificar os arquivos
    private static final Pattern OPERADORAS_PATTERN =
            Pattern.compile("(operadoras?_.+\\.csv|Relatorio_cadop\\.csv)", Pattern.CASE_INSENSITIVE);

    private static final Pattern DEMONSTRACOES_ZIP_PATTERN =
            Pattern.compile("\\dT\\d{4}\\.zip", Pattern.CASE_INSENSITIVE);

    public static void downloadRequiredFiles() {
        try {
            System.out.println("🚀 Iniciando download dos arquivos oficiais da ANS");

            // 1. Cria a pasta data se não existir
            new File("src/main/resources/data/").mkdirs();

            // 2. Operadoras Ativas
            downloadOperadorasFile();

            // 3. Demonstrações Contábeis (2023 e 2024)
            downloadAndProcessDemonstracoesFiles();

            System.out.println("\n✅ Todos os arquivos foram processados com sucesso!");

        } catch (Exception e) {
            System.err.println("\n❌ Erro no processamento:");
            System.err.println("Mensagem: " + e.getMessage());
            printManualInstructions();
            System.exit(1);
        }
    }

    private static void downloadOperadorasFile() throws IOException {
        System.out.println("\n🔎 Buscando arquivo de operadoras ativas");
        String operadorasPath = findLatestFile(
                "operadoras_de_plano_de_saude_ativas",
                OPERADORAS_PATTERN
        );
        downloadFile(ANS_BASE_URL + operadorasPath, "src/main/resources/data/active_operators.csv");
    }

    private static void downloadAndProcessDemonstracoesFiles() throws IOException {
        // Usando Calendar para obter o ano atual de forma compatível
        Calendar calendar = Calendar.getInstance();
        int currentYear = calendar.get(Calendar.YEAR);

        for (int year = currentYear - 2; year < currentYear; year++) {
            System.out.println("\n🔎 Processando demonstrações contábeis para " + year);

            // 1. Baixa todos os ZIPs do ano
            List<String> zipFiles = findZipFilesForYear(year);

            // 2. Processa cada arquivo ZIP
            for (String zipFile : zipFiles) {
                processDemonstracaoZip(zipFile, year);
            }

            // 3. Consolida os dados do ano
            consolidateYearData(year);
        }
    }

    private static List<String> findZipFilesForYear(int year) throws IOException {
        System.out.println("🔍 Procurando arquivos ZIP para " + year);

        Document doc = Jsoup.connect(ANS_BASE_URL + "demonstracoes_contabeis/" + year).timeout(TIMEOUT).get();
        Elements links = doc.select("a[href]");

        List<String> matchedFiles = new ArrayList<>();

        for (Element link : links) {
            String filename = link.attr("href");
            if (DEMONSTRACOES_ZIP_PATTERN.matcher(filename).matches() &&
                    filename.contains(String.valueOf(year))) {
                matchedFiles.add(filename);
                System.out.println("✔️ ZIP encontrado: " + filename);
            }
        }

        if (matchedFiles.isEmpty()) {
            throw new IOException("Nenhum arquivo ZIP encontrado para " + year);
        }

        return matchedFiles;
    }

    private static void processDemonstracaoZip(String zipFilename, int year) throws IOException {
        String zipUrl = ANS_BASE_URL + "demonstracoes_contabeis/" + year + "/" + zipFilename;
        String localZipPath = "src/main/resources/data/temp_" + zipFilename;

        // 1. Baixa o ZIP
        System.out.println("⬇️ Baixando: " + zipUrl);
        downloadFile(zipUrl, localZipPath);

        // 2. Extrai o CSV do ZIP
        System.out.println("📦 Extraindo arquivos de " + zipFilename);
        extractCSVFromZip(localZipPath, year);

        // 3. Remove o ZIP após extração
        Files.deleteIfExists(Paths.get(localZipPath));
    }

    private static void extractCSVFromZip(String zipPath, int year) throws IOException {
        try (ZipInputStream zis = new ZipInputStream(new FileInputStream(zipPath))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (entry.getName().toLowerCase().endsWith(".csv")) {
                    String outputPath = "src/main/resources/data/" +
                            entry.getName().replaceAll(".*/", "") +
                            "_" + year + "_" +
                            zipPath.replaceAll(".*temp_(\\dT).*", "$1") +
                            ".csv";

                    System.out.println("💾 Extraindo: " + entry.getName() + " para " + outputPath);

                    try (FileOutputStream fos = new FileOutputStream(outputPath)) {
                        byte[] buffer = new byte[1024];
                        int len;
                        while ((len = zis.read(buffer)) > 0) {
                            fos.write(buffer, 0, len);
                        }
                    }
                }
            }
        }
    }

    private static void consolidateYearData(int year) throws IOException {
        System.out.println("🧩 Consolidando dados para " + year);
        // Implemente aqui a lógica para juntar os CSVs trimestrais
        // Pode usar OpenCSV para ler todos e gerar um arquivo consolidado
    }

    private static String findLatestFile(String directory, Pattern pattern) throws IOException {
        Document doc = Jsoup.connect(ANS_BASE_URL + directory).timeout(TIMEOUT).get();
        Elements links = doc.select("a[href]");

        List<String> matchedFiles = new ArrayList<>();

        for (Element link : links) {
            String filename = link.attr("href");
            if (pattern.matcher(filename).matches()) {
                matchedFiles.add(filename);
            }
        }

        if (matchedFiles.isEmpty()) {
            throw new IOException("Nenhum arquivo encontrado com o padrão: " + pattern);
        }

        matchedFiles.sort(Collections.reverseOrder());
        return directory + "/" + matchedFiles.get(0);
    }

    private static void downloadFile(String fileUrl, String localPath) throws IOException {
        URL url = new URL(fileUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(TIMEOUT);
        conn.setReadTimeout(TIMEOUT);

        if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
            throw new IOException("HTTP " + conn.getResponseCode() + " - " + conn.getResponseMessage());
        }

        File localFile = new File(localPath);
        try (InputStream in = conn.getInputStream()) {
            Files.copy(in, localFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } finally {
            conn.disconnect();
        }
    }

    private static void printManualInstructions() {
        System.err.println("\n🔍 Instruções para download manual:");
        System.err.println("1. Acesse: https://dadosabertos.ans.gov.br");
        System.err.println("2. Navegue até:");
        System.err.println("   - Operadoras Ativas: " + ANS_BASE_URL + "operadoras_de_plano_de_saude_ativas/");
        System.err.println("   - Demonstrações Contábeis: " + ANS_BASE_URL + "demonstracoes_contabeis/");
        System.err.println("3. Baixe os seguintes arquivos:");
        System.err.println("   - Relatorio_cadop.csv (Operadoras Ativas)");
        System.err.println("   - Arquivos ZIP trimestrais (ex: 1T2023.zip)");
        System.err.println("4. Extraia os CSVs dos ZIPs");
        System.err.println("5. Coloque em: src/main/resources/data/");
    }
}