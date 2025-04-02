package com.ans;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Year;
import java.util.*;
import java.util.regex.Pattern;

public class ANSDataImporter {
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/teste3_database";
    private static final String USER = "postgres";
    private static final String PASS = "password";
    private static final int MAX_RETRIES = 3;
    private static final int BATCH_SIZE = 1000;

    public static void main(String[] args) {
        System.out.println("🏥 Iniciando importação de dados da ANS");

        // Etapa 1: Download e preparação dos arquivos
        FileDownloader.downloadRequiredFiles();

        // Etapa 2: Importação para o PostgreSQL
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
            int totalImported = 0;
            int currentYear = Year.now().getValue();

            // Importa operadoras ativas com tratamento especial
            totalImported += importOperators(
                    conn,
                    "src/main/resources/data/active_operators.csv"
            );

            // Importa demonstrações contábeis
            for (int year = currentYear - 1; year <= currentYear; year++) {
                String consolidatedFile = "src/main/resources/data/financial_reports_" + year + "_consolidated.csv";
                if (consolidateFinancialData(year, consolidatedFile)) {
                    totalImported += importFinancialReports(conn, consolidatedFile);
                }
            }

            // Relatório final
            System.out.println("\n📊 Resultado da Importação:");
            System.out.println("- Total de registros: " + totalImported);
            System.out.println("- Tabelas atualizadas: operators, financial_reports");

        } catch (Exception e) {
            System.err.println("\n❌ Erro durante a importação:");
            e.printStackTrace();
            System.exit(1);
        }
    }

    // Método renomeado de consolidateYearData para consolidateFinancialData
    private static boolean consolidateFinancialData(int year, String outputFile) throws IOException, CsvValidationException {
        System.out.println("\n🧩 Consolidando dados para " + year);

        File dir = new File("src/main/resources/data/");
        File[] files = dir.listFiles((d, name) ->
                name.matches(".*_" + year + "(_\\dT)?.*\\.csv") &&
                        !name.contains("consolidated") &&
                        !name.equals(outputFile)
        );

        if (files == null || files.length == 0) {
            System.err.println("⚠️ Nenhum arquivo encontrado para " + year);
            return false;
        }

        // Ordena os arquivos por data de modificação (do mais recente)
        Arrays.sort(files, (f1, f2) -> Long.compare(f2.lastModified(), f1.lastModified()));

        try (CSVWriter writer = new CSVWriter(new FileWriter(outputFile))) {
            // Escreve cabeçalho (usa o primeiro arquivo como referência)
            try (CSVReader reader = new CSVReader(new FileReader(files[0]))) {
                writer.writeNext(reader.readNext());
            }

            // Consolida todos os arquivos
            for (File file : files) {
                System.out.println("➕ Adicionando: " + file.getName());
                try (CSVReader reader = new CSVReader(new FileReader(file))) {
                    reader.readNext(); // Pula cabeçalho
                    String[] nextLine;
                    while ((nextLine = reader.readNext()) != null) {
                        writer.writeNext(nextLine);
                    }
                }
            }
        }

        System.out.println("✅ Dados consolidados em: " + outputFile);
        return true;
    }

    private static int importOperators(Connection conn, String filePath) throws Exception {
        System.out.println("\n📤 Processando operadoras ativas: " + filePath);

        // Primeiro limpa a tabela para evitar duplicatas
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE TABLE operators CASCADE");
        }

        int recordCount = 0;
        int retryCount = 0;

        // Configuração do parser CSV
        CSVParser parser = new CSVParserBuilder()
                .withSeparator(';')
                .withIgnoreQuotations(false)
                .build();

        while (retryCount < MAX_RETRIES) {
            try (CSVReader reader = new CSVReaderBuilder(new FileReader(filePath))
                    .withCSVParser(parser)
                    .withSkipLines(1) // Pular cabeçalho
                    .build()) {

                String insertSQL = "INSERT INTO operators (ans_registration, cnpj, legal_name, trade_name, " +
                        "modality, address, number, complement, neighborhood, city, state, " +
                        "zip_code, area_code, phone, fax, email, representative, " +
                        "representative_role, created_at) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                    String[] nextLine;
                    while ((nextLine = reader.readNext()) != null) {
                        if (nextLine.length < 19) {
                            System.err.println("⚠️ Linha ignorada (colunas insuficientes): " + Arrays.toString(nextLine));
                            continue;
                        }

                        try {
                            // Define cada parâmetro individualmente
                            for (int i = 0; i < nextLine.length; i++) {
                                String value = nextLine[i].replace("\"", "").trim();
                                pstmt.setString(i + 1, value.isEmpty() ? null : value);
                            }

                            // Tratamento especial para o campo created_at (posição 19)
                            String createdAtStr = nextLine[18].replace("\"", "").trim(); // Pega a data do CSV
                            if (!createdAtStr.isEmpty()) {
                                try {
                                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                    java.util.Date parsedDate = dateFormat.parse(createdAtStr);
                                    Date createdAt = new Date(parsedDate.getTime());
                                    pstmt.setDate(19, createdAt);
                                } catch (ParseException e) {
                                    System.err.println("⚠️ Erro ao converter a data: " + createdAtStr);
                                    pstmt.setNull(19, Types.DATE);
                                }
                            } else {
                                pstmt.setNull(19, Types.DATE);
                            }

                            pstmt.addBatch();
                            recordCount++;

                            if (recordCount % BATCH_SIZE == 0) {
                                pstmt.executeBatch();
                                System.out.print("⏳ " + recordCount + " registros...");
                            }
                        } catch (ArrayIndexOutOfBoundsException e) {
                            System.err.println("⚠️ Linha com formato inválido: " + Arrays.toString(nextLine));
                        }
                    }
                    pstmt.executeBatch(); // Executa o lote final
                }
                break; // Sai do loop se bem-sucedido
            } catch (SQLException e) {
                retryCount++;
                if (retryCount >= MAX_RETRIES) {
                    throw e;
                }
                System.err.println("⚠️ Tentativa " + retryCount + " falhou. Tentando novamente...");
                Thread.sleep(3000); // Espera 3 segundos antes de tentar novamente
            }
        }

        System.out.println("\n✔️ " + recordCount + " operadoras importadas");
        return recordCount;
    }

    private static int importFinancialReports(Connection conn, String filePath) throws Exception {
        System.out.println("\n📤 Processando demonstrações financeiras: " + filePath);

        // Primeiro limpa os dados do ano correspondente
        try (Statement stmt = conn.createStatement()) {
            String year = extractYearFromFileName(filePath);
            if (year != null) {
                stmt.execute("DELETE FROM financial_reports WHERE EXTRACT(YEAR FROM report_date) = " + year);
            }
        }

        int recordCount = 0;

        // Configuração do parser CSV
        CSVParser parser = new CSVParserBuilder()
                .withSeparator(';')
                .withIgnoreQuotations(true)
                .build();

        try (CSVReader reader = new CSVReaderBuilder(new FileReader(filePath))
                .withCSVParser(parser)
                .withSkipLines(1) // Pular cabeçalho
                .build()) {

            String insertSQL = "INSERT INTO financial_reports (report_date, operator_code, account, description, " +
                    "is_consolidated, balance) VALUES (?, ?, ?, ?, ?, ?)";

            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                String[] nextLine;
                while ((nextLine = reader.readNext()) != null) {
                    if (nextLine.length < 6) {
                        System.err.println("⚠️ Linha ignorada: " + Arrays.toString(nextLine));
                        continue;
                    }

                    try {
                        // Data do relatório (usa java.sql.Date explicitamente)
                        pstmt.setDate(1, java.sql.Date.valueOf(nextLine[0].replace("\"", "").trim()));

                        // Código da operadora
                        pstmt.setString(2, nextLine[1].replace("\"", "").trim());

                        // Conta
                        pstmt.setString(3, nextLine[2].replace("\"", "").trim());

                        // Descrição
                        pstmt.setString(4, nextLine[3].replace("\"", "").trim());

                        // Consolidado (0 ou 1)
                        pstmt.setInt(5, Integer.parseInt(nextLine[4].trim()));

                        // Ajusta o formato do valor numérico, removendo aspas e espaçamentos extras
                        String balanceStr = nextLine[5].replace("\"", "").replace(".", "").replace(",", ".").trim();
                        try {
                            pstmt.setBigDecimal(6, new BigDecimal(balanceStr));
                        } catch (NumberFormatException e) {
                            System.err.println("⚠️ Erro ao converter o valor: " + balanceStr);
                            pstmt.setBigDecimal(6, BigDecimal.ZERO); // Evita falha ao inserir dados
                        }


                        pstmt.addBatch();
                        recordCount++;

                        if (recordCount % BATCH_SIZE == 0) {
                            pstmt.executeBatch();
                            System.out.print("⏳ " + recordCount + " registros...");
                        }
                    } catch (Exception e) {
                        System.err.println("⚠️ Erro ao processar linha: " + Arrays.toString(nextLine));
                        e.printStackTrace();
                    }
                }
                pstmt.executeBatch(); // Executa o lote final
            }
        }

        System.out.println("\n✔️ " + recordCount + " demonstrações financeiras importadas");
        return recordCount;
    }

    private static String extractYearFromFileName(String fileName) {
        Pattern pattern = Pattern.compile(".*_(\\d{4})_.*");
        java.util.regex.Matcher matcher = pattern.matcher(fileName);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }
}