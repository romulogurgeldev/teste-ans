package com.ans;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import com.opencsv.CSVWriter;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.model.FileHeader;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.*;

public class PdfToCsv {
    public static void main(String[] args) {
        final String ZIP_PATH = "../web-scraping/anexos.zip";
        final String CSV_FILE = "procedimentos.csv";
        final String OUTPUT_ZIP = "Teste_Romulo_Gurgel.zip";
        final String DEBUG_FILE = "pdf_debug.txt";

        try {
            // 1. Extrair e validar PDF
            String pdfPath = extractPdf(ZIP_PATH);

            // 2. Processamento com múltiplas estratégias
            List<String[]> tableData = extractData(pdfPath, DEBUG_FILE);

            if (tableData.isEmpty()) {
                throw new IOException("""
                    Nenhum dado extraído. Verifique:
                    1. O arquivo """ + DEBUG_FILE + " para ver a estrutura real\n" +
                        "2. Se o PDF contém tabela em formato texto (não é imagem)");
            }

            // 3. Gerar arquivos de saída
            generateCsv(CSV_FILE, tableData);
            createZip(OUTPUT_ZIP, CSV_FILE);

            System.out.println("\n✅ Conversão concluída com sucesso!");
            System.out.println("Registros extraídos: " + tableData.size());
            System.out.println("Arquivo final: " + OUTPUT_ZIP);

        } catch (Exception e) {
            System.err.println("\n❌ Erro: " + e.getMessage());
            System.err.println("Consulte " + DEBUG_FILE + " para diagnóstico");
        }
    }

    // Extrai o PDF do ZIP com validação
    private static String extractPdf(String zipPath) throws IOException {
        ZipFile zipFile = new ZipFile(zipPath);
        for (FileHeader header : zipFile.getFileHeaders()) {
            String fileName = header.getFileName().toLowerCase();
            if (fileName.contains("anexo_i") && fileName.endsWith(".pdf")) {
                zipFile.extractFile(header.getFileName(), "./");
                System.out.println("PDF extraído: " + header.getFileName());
                return header.getFileName();
            }
        }
        throw new FileNotFoundException("Arquivo 'Anexo_I.pdf' não encontrado no ZIP");
    }

    // Método principal de extração com fallback
    private static List<String[]> extractData(String pdfPath, String debugPath) throws IOException {
        // Gera arquivo de diagnóstico
        String pdfContent = getPdfContent(pdfPath);
        Files.writeString(Path.of(debugPath), pdfContent, StandardOpenOption.CREATE);
        System.out.println("Arquivo de diagnóstico gerado: " + debugPath);

        // Tenta diferentes estratégias
        List<String[]> data = tryStandardExtraction(pdfContent);
        if (data.isEmpty()) data = tryAdvancedExtraction(pdfContent);
        if (data.isEmpty()) data = tryFallbackExtraction(pdfContent);

        return data;
    }

    // Extrai conteúdo completo do PDF
    private static String getPdfContent(String pdfPath) throws IOException {
        try (PDDocument doc = PDDocument.load(new File(pdfPath))) {
            PDFTextStripper stripper = new PDFTextStripper();
            stripper.setSortByPosition(true);
            return stripper.getText(doc);
        }
    }

    // Estratégia 1: Para tabelas bem formatadas
    private static List<String[]> tryStandardExtraction(String text) {
        List<String[]> data = new ArrayList<>();
        Pattern pattern = Pattern.compile("^(\\d{4,})\\s+(.+?)\\s+(OD|AMB)\\s*$");

        for (String line : text.split("\\r?\\n")) {
            Matcher m = pattern.matcher(line.trim());
            if (m.matches()) {
                data.add(createRow(m.group(1), m.group(2), m.group(3)));
            }
        }
        return data;
    }

    // Estratégia 2: Para PDFs com formatação irregular
    private static List<String[]> tryAdvancedExtraction(String text) {
        List<String[]> data = new ArrayList<>();
        Pattern pattern = Pattern.compile("(\\d{4,})[\\s|\\t]+(.+?)[\\s|\\t]+(OD|AMB)");

        for (String line : text.split("\\r?\\n")) {
            Matcher m = pattern.matcher(line.trim());
            if (m.find()) {
                data.add(createRow(m.group(1), m.group(2), m.group(3)));
            }
        }
        return data;
    }

    // Estratégia 3: Fallback para casos extremos
    private static List<String[]> tryFallbackExtraction(String text) {
        List<String[]> data = new ArrayList<>();
        Pattern codePattern = Pattern.compile("\\d{4,}");
        Pattern typePattern = Pattern.compile("\\b(OD|AMB)\\b");

        for (String line : text.split("\\r?\\n")) {
            line = line.replaceAll("\\s+", " ").trim();
            if (codePattern.matcher(line).find() && typePattern.matcher(line).find()) {
                String[] parts = line.split("\\s{2,}|\\t");
                if (parts.length >= 3) {
                    data.add(createRow(parts[0], parts[1], parts[2]));
                }
            }
        }
        return data;
    }

    private static String[] createRow(String code, String desc, String type) {
        return new String[]{
                code.trim(),
                desc.trim(),
                type.equals("OD") ? "Odontológico" : "Ambulatorial"
        };
    }

    private static void generateCsv(String csvPath, List<String[]> data) throws IOException {
        try (CSVWriter writer = new CSVWriter(new FileWriter(csvPath))) {
            writer.writeNext(new String[]{"Código", "Descrição", "Tipo"});
            writer.writeAll(data);
        }
    }

    private static void createZip(String zipPath, String fileToZip) throws IOException {
        new ZipFile(zipPath).addFile(fileToZip);
    }
}