package com;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.StringTokenizer;

public class RowSwapServer implements Runnable {

    private static final int INV_ERR = 1;
    private static final int ARG_ERR = 2;
    private static final int SOCKET_ERR = 3;
    private static final int FILE_ERR = 4;
    private static final int ENCODE_DS_ERR = 5;
    private static final int RECEIVE_DS_ERR = 6;
    private static final int DECODE_DS_ERR = 7;
    private static final int REG_ERR = 8;
    private static final int JOIN_ERR = 9;
    private static final int ENCODE_CLIENT_ERR = 10;
    private static final int RECEIVE_CLIENT_ERR = 11;
    private static final int DECODE_CLIENT_ERR = 12;
    private static final int SEND_ERR = 13;

    //Protocollo DS <--> RowSwapServer
    //Formato richiesta: CMD:FILENAME:IP:PORT
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

    public static String getResultString(int result) {
        switch (result) {
            case RESULT_OK:
                return "OK";
            case RESULT_MALFORMED_REQUEST:
                return "RICHIESTA MALFORMATA";
            case RESULT_UNKNOWN_COMMAND:
                return "COMANDO SCONOSCIUTO";
            //Errori REGISTER
            case RESULT_FILENAME_IN_USE:
                return "FILENAME IN USO";
            case RESULT_PORT_IN_USE:
                return "PORTA RS IN USO";
            //Errori DISMISS
            case RESULT_FILENAME_NOT_IN_USE:
                return "FILENAME NON IN USO";
            case RESULT_PORT_NOT_CONSISTENT:
                return "PORTA NON COINCIDENTE CON FILENAME";

            default:
                return null;
        }
    }

    private static boolean isPortValid(int port) {
        return !(port < 1024 || port > 65536);
    }

    private final Thread myThread;

    private final DatagramSocket socket;
    private final DatagramPacket packet;
    private final byte[] buf = new byte[256];

    private final InetAddress addressDS;
    private final int portDS;

    private final InetAddress addressRS;
    private final int portRS;

    private final String filename;
    private final Path filePath;

    private boolean isFileValid = false;
    private int fileLineCount = -1;

    private boolean isRegistered = false;
    private int discoveryResult = -1;

    private final Random rnd = new Random();

    public RowSwapServer(InetAddress addressDS, int portDS, int portRS, String filename) throws SocketException, UnknownHostException {
        this(addressDS, portDS, InetAddress.getLocalHost(), portRS, filename);
    }

    public RowSwapServer(InetAddress addressDS, int portDS, InetAddress addressRS, int portRS, String filename) throws SocketException {
        this.addressDS = addressDS;
        this.portDS = portDS;
        this.addressRS = addressRS;
        this.portRS = portRS;
        this.filename = filename;

        this.myThread = new Thread(this);
        this.socket = new DatagramSocket(portRS);
        this.packet = new DatagramPacket(buf, 0, buf.length);
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

    public InetAddress getAddressDS() {
        return addressDS;
    }

    public int getPortDS() {
        return portDS;
    }

    public String getFilename() {
        return filename;
    }

    public int getPortRS() {
        return portRS;
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

    public boolean isRegistered() {
        return isRegistered;
    }

    public InetAddress getAddressRS() {
        return addressRS;
    }

    public int getDiscoveryResult() {
        return discoveryResult;
    }

    public String getDiscoveryResultString() {
        return getResultString(getDiscoveryResult());
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

    public void registerOnDiscovery() {
        if (!isRegistered()) {
            //Apro una richiesta di registrazione al discovery.

            try (ByteArrayOutputStream boStream = new ByteArrayOutputStream(); DataOutputStream doStream = new DataOutputStream(boStream)) {
                doStream.writeUTF(CMD_REGISTER + ":" + addressRS.getHostAddress() + ":" + portRS);

                packet.setAddress(addressDS);
                packet.setPort(portRS);
                packet.setData(boStream.toByteArray());
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(ENCODE_DS_ERR);
            }

            //Invio il pacchetto
            try {
                socket.send(packet);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(SEND_ERR);
            }

            //Attendo risposta.
            try {
                packet.setData(buf, 0, buf.length);
                socket.receive(packet);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(RECEIVE_DS_ERR);
            }

            //Decodifico la risposta.
            try (ByteArrayInputStream biStream = new ByteArrayInputStream(packet.getData()); DataInputStream diStream = new DataInputStream(biStream)) {
                //leggo nome file inviato dal client --> risponderò con la corrispettiva porta (se corretto)
                discoveryResult = diStream.readInt();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(DECODE_DS_ERR);
            }

            isRegistered = discoveryResult == RESULT_OK;
        }
    }

    public void dismissFromDiscovery() {
        if (isRegistered()) {
            //Apro una richiesta di cancellazione al discovery.

            try (ByteArrayOutputStream boStream = new ByteArrayOutputStream(); DataOutputStream doStream = new DataOutputStream(boStream)) {
                doStream.writeUTF(CMD_DISMISS + ":" + addressRS.getHostAddress() + ":" + portRS);

                packet.setAddress(addressDS);
                packet.setPort(portRS);
                packet.setData(boStream.toByteArray());
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(ENCODE_DS_ERR);
            }

            //Invio il pacchetto
            try {
                socket.send(packet);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(SEND_ERR);
            }

            //Attendo risposta.
            try {
                packet.setData(buf, 0, buf.length);
                socket.receive(packet);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(RECEIVE_DS_ERR);
            }

            //Decodifico la risposta.
            try (ByteArrayInputStream biStream = new ByteArrayInputStream(packet.getData()); DataInputStream diStream = new DataInputStream(biStream)) {
                //leggo nome file inviato dal client --> risponderò con la corrispettiva porta (se corretto)
                discoveryResult = diStream.readInt();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(DECODE_DS_ERR);
            }

            //Anche se il discovery mi da errore lo interpreto come una cancellazione.
            isRegistered = !(discoveryResult == RESULT_OK || discoveryResult == RESULT_FILENAME_NOT_IN_USE || discoveryResult == RESULT_PORT_NOT_CONSISTENT);
        }
    }

    @Override
    public void run() {
        //Metodo principale del server:
        //Cosa fa?
        //1) REPL dei client
        //2) In caso di eccezione termina con un errore: se l'errore non è critico rilancio il server con script esterno.

        while (true) {
            String richiesta = null;
            String esito = null;

            //ciclicamente risetto il buffer del pacchetto
            packet.setData(buf, 0, buf.length);

            try {
                socket.receive(packet); //attendo una richiesta da un client
            } catch (IOException e) {
                //non dovrebbe entrare se non impostato timeout.
                e.printStackTrace();
                System.exit(RECEIVE_CLIENT_ERR);
            }

            try (ByteArrayInputStream biStream = new ByteArrayInputStream(packet.getData(), 0, packet.getLength()); DataInputStream diStream = new DataInputStream(biStream)){
                richiesta = diStream.readUTF(); //leggo le due righe separate da virgola
            } catch (IOException e) {
                e.printStackTrace();
                //System.exit(DECODE_CLIENT_ERR);
                esito = "richiesta malformata";
            }

            //Continuo solo la decodifica è andata a buon fine.
            if(esito == null){
                StringTokenizer st = new StringTokenizer(richiesta, ","); //splitto per trovare le due righe da scambiare

                try{
                    int riga1 = Integer.parseInt(st.nextToken());
                    int riga2 = Integer.parseInt(st.nextToken());

                    //Scambio le righe e ritorno l'esito.
                    esito = swap(riga1, riga2);

                } catch (NumberFormatException | NoSuchElementException e){
                    esito = "righe malformate";
                }
            }

            try (ByteArrayOutputStream boStream = new ByteArrayOutputStream(); DataOutputStream doStream = new DataOutputStream(boStream)) {
                //rispondo con esito dell'operazione di swap
                doStream.writeUTF(esito);
                packet.setData(boStream.toByteArray());
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(ENCODE_CLIENT_ERR);
            }

            try {
                //invio la risposta con esito dello swap
                socket.send(packet);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(SEND_ERR);
            }

        }

        //Rilascio risorse
        //if(isRegistered()) dismissDiscovery();
        //socket.close();

    }

    public static void main(String[] args) {
        //RS IPDS portDS portRS nomeFile

        //controllo che l'utente abbia inserito portaDS e almeno un file e una porta
        if (args.length != 4) {
            System.out.println("RS IPDS portDS portRS nomeFile");
            System.exit(INV_ERR);
        }

        //Check dell'IP DS.
        InetAddress addressDS = null;

        try {
            addressDS = InetAddress.getByName(args[0]);
        } catch (UnknownHostException e) {
            System.err.println("IPDS malformato");
            System.exit(ARG_ERR);
        }

        //Check delle porte
        int portDS = -1;
        int portRS = -1;

        try {
            portDS = Integer.parseInt(args[1]);
            portRS = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.err.println("portDS o portRS malformato");
            System.exit(ARG_ERR);
        }

        if (!isPortValid(portDS)) {
            System.err.println("portDS non valida");
            System.exit(ARG_ERR);
        }

        if (!isPortValid(portRS)) {
            System.err.println("portRS non valida");
            System.exit(ARG_ERR);
        }

        //Oggetto server.
        RowSwapServer server = null;

        try {
            server = new RowSwapServer(addressDS, portDS, portRS, args[3]);
        } catch (SocketException | UnknownHostException e) {
            System.err.println("Impossibile iniz. socket");
            System.exit(SOCKET_ERR);
        }

        server.checkFileValidity();
        server.countLines();

        if (!server.isFileValid()) {
            System.err.println("Impossibile aprire il r/w il file " + server.getFilename());
            System.exit(FILE_ERR);
        }

        System.out.println("Server inizializzato.");
        System.out.println("Provo a registrarmi sul DS " + server.getAddressDS().getHostAddress() + ":" + server.getPortDS());

        server.registerOnDiscovery();

        if (!server.isRegistered()) {
            System.err.println(server.getDiscoveryResultString());
            System.exit(REG_ERR);
        }

        //Avvio il server
        server.start();

        System.out.println("Server avviato.");
        System.out.println("IP: " + server.getAddressRS().getHostAddress());
        System.out.println("Porta: " + server.getPortRS());
        System.out.println("Nome file: " + server.getFilename());
        System.out.println("Path: " + server.getFilePath());
        System.out.println("Numero righe: " + server.getFileLineCount());
        System.out.println("Attendo terminazione del figlio...");

        try {
            server.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(JOIN_ERR);
        }

    }

}
