package com;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Random;
import java.util.StringTokenizer;

public class RowSwapServer implements Runnable {

    private static final int RS_RECEIVE_ERR = 1;
    private static final int RS_READ_UTF_ERR = 2;
    private static final int RS_WRITE_UTF_ERR = 3;
    private static final int RS_SEND_ERR = 4;

    //Protocollo DS <--> RowSwapServer
    //Formato richiesta: CMD:FILENAME:PORT
    //Formato risposta: intero

    private static final String CMD_REGISTER = "REGISTER";
    private static final String CMD_DISMISS = "DISMISS";

    private static final int RESULT_OK = 0;
    private static final int RESULT_MALFORMED_REQUEST = 1;
    private static final int RESULT_UNKNOWN_COMMAND = 2;
    private static final int RESULT_FILENAME_IN_USE = 3;
    private static final int RESULT_PORT_IN_USE = 4;
    private static final int RESULT_FILENAME_NOT_IN_USE = 5;
    private static final int RESULT_PORT_NOT_CONSISTENT = 6;

    private final Thread myThread;

    private final DatagramSocket socket;
    private final DatagramPacket packet;
    private final byte[] buf = new byte[256];

    private final String filename;
    private final int port;
    private final Path filePath;

    private boolean isFileValid = false;
    private int fileLineCount = -1;

    private final Random rnd = new Random();

    public RowSwapServer(String filename, int port) throws SocketException {
        this.myThread = new Thread(this);
        this.socket = new DatagramSocket(port);
        this.packet = new DatagramPacket(buf, 0, buf.length);
        this.filename = filename;
        this.port = port;
        this.filePath = Path.of(new File(filename).toURI());

        //Devo impostare questa opzione, in modo che lo script esterno riesca a riavviare il server.
        this.socket.setReuseAddress(true);
    }

    public void checkFileValidity() {
        if (!Files.exists(filePath)) {
            System.err.println("Il file " + filename + " non esite.");
            isFileValid = false;
            return;
        }

        //Controllo che il file sia accessibile sia in lettura sia in scrittura.

        if (!Files.isReadable(filePath) || !Files.isWritable(filePath)) {
            System.err.println("Il file " + filename + " non è leggibile/scrivibile.");
            isFileValid = false;
            return;
        }

        isFileValid = true;
    }

    public void countLines() {
        //Conto le righe.
        if (isFileValid) {
            try (BufferedReader bufferedReader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)) {
                int tmp = 0;

                while (bufferedReader.readLine() != null) tmp++;
                //setto all'interno della struttura il numero di righe per ciascun file
                fileLineCount = tmp;

            } catch (IOException e) {
                System.err.println("Errore nell'aprire il file: " + e.getMessage());
                //Il prof ha detto che non si butta via nulla.
                //System.exit(IO_ERROR);
                isFileValid = false;
            }
        }
    }

    public void start() {
        myThread.start();
    }

    public void join() throws InterruptedException {
        myThread.join();
    }

    public void join(long millis) throws InterruptedException {
        myThread.join(millis);
    }

    public String getFilename() {
        return filename;
    }

    public int getPort() {
        return port;
    }

    public Path getFilePath() {
        return filePath;
    }

    public boolean isFileValid() {
        return isFileValid;
    }

    public int getFileLineCount() {
        return fileLineCount;
    }

    private String swap(int riga1, int riga2) {

        final String esitoOK = "OK";

        //Giustamente il controllo viene fatto a livello client...
        //In questo caso isolato può anche andare, ma in un contesto più generale
        //dove il client viene implementato da terze parti, non sappiamo se hanno fatto il controllo.
        //if(riga1 == riga2) return esitoOK;

        //Controllo sulle righe (se superano la dimensione del file su cui insisto non ci provo nemmeno ritorno stringa errore
        if (riga1 > fileLineCount || riga2 > fileLineCount) {
            return "Riga 1 o Riga 2 supera la dimensione del file. (" + fileLineCount + ")";
        }

        //Cerco la riga più in basso:
        //Nel ciclo mi fermerò lì.
        int maxLine = riga1 > riga2 ? riga1 : riga2;

        //Leggo tutto il file e mi salvo le righe da swappare
        String inDaSwap1 = null;
        String inDaSwap2 = null;

        try (BufferedReader bufferedReader = Files.newBufferedReader(filePath)) {
            //Leggo le righe.
            for (int i = 0; i < fileLineCount; i++) {
                String tmpLine = bufferedReader.readLine();

                if (i == riga1) inDaSwap1 = tmpLine;
                if (i == riga2) inDaSwap2 = tmpLine;
                if (i == maxLine) break;

            }
        } catch (IOException e) {
            String err = "Errore nell'aprire il file: " + e.getMessage();
            System.err.println(err);
            return err;
            //non esco ma rispondo con una stringa che rappresenta il problema
        }

        //Buffer temporaneo del file temporaneo.
        //Path tmpPath = Paths.get(new File(getId() + ".tmp").toURI());
        //Disponibile da Java 11
        Path tmpPath = Path.of(new File(rnd.nextInt() + ".tmp").toURI());

        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(tmpPath, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {

            try (BufferedReader bufferedReader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)) {
                //Leggo le righe.
                for (int i = 0; i < fileLineCount; i++) {
                    String tmpLine = bufferedReader.readLine();

                    if (i == riga1) { //se la riga letta è quella di indice riga1 allora ci scrivo la seconda
                        bufferedWriter.write(inDaSwap2);
                    } else if (i == riga2) { //se la riga letta è quella di indice riga2 allora ci scrivo la prima
                        bufferedWriter.write(inDaSwap1);
                    } else {
                        bufferedWriter.write(tmpLine);
                    }
                    bufferedWriter.newLine(); //dopo aver scritto la riga stampo il fine linea
                }
            } catch (IOException e) {
                String err = "Errore nell'aprire il file: " + e.getMessage();
                System.err.println(err);
                return err;
            }

        } catch (IOException e) {
            String err = "Impossibile creare il file temporaneo: " + e.getMessage();
            System.err.println(err);
            return err;
        }

        try {
            //sposto il file tmp in quello finale
            Files.move(tmpPath, filePath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            String err = "Impossibile spostare il file temporaneo: " + e.getMessage();
            System.err.println(err);
            return err;
        }
        //ritorno esito in formato di stringa dello swap
        return esitoOK;
    }

    @Override
    public void run() {




    }

    public static void main(String[] args) {

    }

}
