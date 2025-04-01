package com.ans;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import net.lingala.zip4j.ZipFile;
import java.io.*;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class App {
    public static void main(String[] args) {
        String baseUrl = "https://www.gov.br/ans/pt-br/acesso-a-informacao/participacao-da-sociedade/atualizacao-do-rol-de-procedimentos";

        try {
            // Buscar os links reais dos PDFs
            List<String> pdfLinks = getPdfLinks(baseUrl);

            if (pdfLinks.isEmpty()) {
                System.out.println("Nenhum PDF encontrado para download!");
                return;
            }

            // Criar diretório de downloads
            File downloadDir = new File("downloads");
            if (!downloadDir.exists()) {
                downloadDir.mkdir();
            }

            // Baixar PDFs em paralelo
            List<File> downloadedFiles = new ArrayList<>();
            List<CompletableFuture<Void>> downloadTasks = new ArrayList<>();

            for (String pdfUrl : pdfLinks) {
                String fileName = "downloads/" + pdfUrl.substring(pdfUrl.lastIndexOf("/") + 1);
                downloadedFiles.add(new File(fileName));
                downloadTasks.add(CompletableFuture.runAsync(() -> downloadPDF(pdfUrl, fileName)));
            }

            CompletableFuture.allOf(downloadTasks.toArray(new CompletableFuture[0])).join();

            // Compactar os arquivos baixados
            new ZipFile("anexos.zip").addFiles(downloadedFiles);

            System.out.println("Arquivos baixados e compactados com sucesso!");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<String> getPdfLinks(String url) {
        List<String> pdfLinks = new ArrayList<>();
        try {
            Document doc = Jsoup.connect(url).get();
            Elements links = doc.select("a[href$=.pdf]");

            for (Element link : links) {
                String absUrl = link.absUrl("href");
                if (!absUrl.isEmpty()) {
                    pdfLinks.add(absUrl);
                }
            }
        } catch (IOException e) {
            System.err.println("Erro ao buscar links de PDFs: " + e.getMessage());
        }
        return pdfLinks;
    }

    private static void downloadPDF(String pdfUrl, String fileName) {
        try {
            URL url = new URL(pdfUrl);
            ReadableByteChannel rbc = Channels.newChannel(url.openStream());
            FileOutputStream fos = new FileOutputStream(fileName);
            fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
            fos.close();
            System.out.println("Download concluído: " + fileName);
        } catch (IOException e) {
            System.err.println("Erro ao baixar " + fileName + ": " + e.getMessage());
        }
    }
}
