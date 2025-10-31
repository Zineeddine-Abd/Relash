package DiskManager;

import Main.DBConfig;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class DiskManager {

    private final DBConfig config;
    private final List<PageId> freePages;
    private final String binDataPath;

    public DiskManager(DBConfig config) {
        this.config = config;
        this.freePages = new ArrayList<>();
        this.binDataPath = config.getDbPath() + File.separator + "BinData";
    }

    public void Init() {
        try {
            Path binDataDir = Paths.get(binDataPath);
            if (!Files.exists(binDataDir)) {
                Files.createDirectories(binDataDir);
            }
            loadFreePages();
        } catch (IOException e) {
            System.err.println("Erreur initialisation DiskManager: " + e.getMessage());
        }
    }

    public void Finish() {
        saveFreePages();
    }

    public PageId AllocPage() {
        if (!freePages.isEmpty()) {
            return freePages.remove(0);
        }

        try {
            for (int fileIdx = 0; fileIdx < config.getDmMaxFileCount(); fileIdx++) {
                File dataFile = new File(binDataPath + File.separator + "Data" + fileIdx + ".bin");

                if (!dataFile.exists()) {
                    dataFile.createNewFile();
                }

                long fileSize = dataFile.length();
                int currentPages = (int) (fileSize / config.getPageSize());

                if (currentPages < config.getDmMaxPagesPerFile()) {

                    try (RandomAccessFile raf = new RandomAccessFile(dataFile, "rw")) {
                        raf.setLength(fileSize + config.getPageSize());
                    }
                    return new PageId(fileIdx, currentPages);
                }
            }
            throw new RuntimeException("Nombre maximum de fichiers atteint");
        } catch (IOException e) {
            throw new RuntimeException("Erreur allocation page: " + e.getMessage());
        }
    }

    public void ReadPage(PageId pageId, ByteBuffer buffer) {
        try {
            String fileName = binDataPath + File.separator + "Data" + pageId.getFileIdx() + ".bin";
            File file = new File(fileName);

            if (!file.exists()) {
                // Si le fichier n'existe pas, c'est une page vide, on la remplit de zéros
                buffer.clear();
                buffer.put(new byte[config.getPageSize()]);
                buffer.flip();
                return;
            }

            RandomAccessFile raf = new RandomAccessFile(file, "r");
            long offset = (long) pageId.getPageIdx() * config.getPageSize();

            // Vérifier si l'offset est au-delà de la fin du fichier (page "logiquement"
            // allouée mais jamais écrite)
            if (offset >= raf.length()) {
                buffer.clear();
                buffer.put(new byte[config.getPageSize()]);
                buffer.flip();
                raf.close();
                return;
            }

            raf.seek(offset);
            byte[] data = new byte[config.getPageSize()];
            int bytesRead = raf.read(data);
            int startIndex = (bytesRead == -1) ? 0 : bytesRead;

            if (startIndex < config.getPageSize()) {
                for (int i = startIndex; i < config.getPageSize(); i++) {
                    data[i] = 0;
                }
            }

            buffer.clear();
            buffer.put(data);
            buffer.flip();
            raf.close();
        } catch (IOException e) {
            throw new RuntimeException("Erreur lecture page: " + e.getMessage());
        }
    }

    public void WritePage(PageId pageId, ByteBuffer buffer) {
        try {
            String fileName = binDataPath + "/Data" + pageId.getFileIdx() + ".bin";
            File file = new File(fileName);

            if (!file.exists()) {
                file.createNewFile();
            }

            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            long offset = (long) pageId.getPageIdx() * config.getPageSize();
            raf.seek(offset);

            byte[] data = new byte[config.getPageSize()];
            buffer.position(0);
            buffer.get(data, 0, Math.min(buffer.remaining(), config.getPageSize()));

            raf.write(data);
            raf.close();

        } catch (IOException e) {
            throw new RuntimeException("Erreur écriture page: " + e.getMessage());
        }
    }

    public void DeallocPage(PageId pageId) {
        if (!freePages.contains(pageId)) {
            freePages.add(pageId);
        }
    }

    private void loadFreePages() {
        File metaFile = new File(binDataPath + "/freepages.meta");
        if (!metaFile.exists()) {
            return;
        }

        try (BufferedReader br = new BufferedReader(new FileReader(metaFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 2) {
                    int fileIdx = Integer.parseInt(parts[0]);
                    int pageIdx = Integer.parseInt(parts[1]);
                    freePages.add(new PageId(fileIdx, pageIdx));
                }
            }
        } catch (IOException e) {
            System.err.println("Erreur chargement pages libres: " + e.getMessage());
        }
    }

    private void saveFreePages() {
        File metaFile = new File(binDataPath + "/freepages.meta");

        try (FileWriter writer = new FileWriter(metaFile)) {
            for (PageId pageId : freePages) {
                writer.write(pageId.getFileIdx() + "," + pageId.getPageIdx() + "\n");
            }
        } catch (IOException e) {
            System.err.println("Erreur sauvegarde pages libres: " + e.getMessage());
        }
    }
}