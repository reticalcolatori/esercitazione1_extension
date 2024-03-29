package com;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

public class DiscoveryServer {

    private static final int INV_ERR = 1;
    private static final int INVALID_DS_PORT = 2;
    private static final int SOCKET_ERR = 3;
    private static final int RECEIVE_ERR = 4;
    private static final int DECODE_REQUEST_ERR = 5;
    private static final int ENCODE_RESPONSE_ERR = 6;
    private static final int SEND_ERR = 7;
    private static final int JOIN_ERR = 8;

    //Protocollo DS <--> RowSwapServer
    //Formato richiesta: CMD:FILENAME:IP:PORT
    //Formato risposta: intero

    private static final String CMD_REGISTER = "REGISTER";
    private static final String CMD_DISMISS = "DISMISS";

    private static final int RESULT_OK = 0;
    private static final int RESULT_MALFORMED_REQUEST = 1;
    private static final int RESULT_UNKNOWN_COMMAND = 2;
    private static final int RESULT_FILENAME_IN_USE = 3;
    private static final int RESULT_PAIR_IN_USE = 4;
    private static final int RESULT_FILENAME_NOT_IN_USE = 5;
    private static final int RESULT_PAIR_NOT_CONSISTENT = 6;


    private static boolean isPortValid(int port) {
        return !(port < 1024 || port > 65536);
    }

    private class DSClientHandler implements Runnable {

        private final Thread myThread;

        private final DiscoveryServer reference;

        private final int port;
        private final DatagramSocket socket;
        private final DatagramPacket packet;
        private final byte buf[] = new byte[256];

        public DSClientHandler(DiscoveryServer reference, int clientPort) throws SocketException {
            this.reference = reference;
            this.myThread = new Thread(this);
            this.port = clientPort;
            this.socket = new DatagramSocket(clientPort);
            this.packet = new DatagramPacket(buf, 0, buf.length);
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

        public int getPort() {
            return port;
        }

        @Override
        public void run() {
            //preparo strutture per lettura/scrittura dati
            String richiesta = null;

            while (true) {
                packet.setData(buf, 0, buf.length); //devo risettare ciclicamente il buffer del pacchetto

                try {
                    //mi pongo in attesa di un packet da parte di un client
                    socket.receive(packet);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(RECEIVE_ERR);
                }

                try (ByteArrayInputStream biStream = new ByteArrayInputStream(packet.getData()); DataInputStream diStream = new DataInputStream(biStream)) {
                    //leggo nome file inviato dal client --> risponderò con la corrispettiva porta (se corretto)
                    richiesta = diStream.readUTF();
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(DECODE_REQUEST_ERR);
                }

                try (ByteArrayOutputStream boStream = new ByteArrayOutputStream(); DataOutputStream doStream = new DataOutputStream(boStream)) {
                    InetAddressPortWrapper wrapper = getAddressPortByFilename(richiesta); //trovo porta corrisp. se esiste
                    if (wrapper == null) { //se il file non esiste lo comunico
                        doStream.writeUTF("Il file richiesto non esiste, quindi non c'è una porta corrispondente\n");
                    } else { //altrimenti restituisco la porta corrisp.
                        doStream.writeUTF(wrapper.getAddress().getHostAddress() + ":" + wrapper.getPort());
                    }

                    //setto il contenuto della risposta
                    packet.setData(boStream.toByteArray());
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(ENCODE_RESPONSE_ERR);
                }

                try {
                    socket.send(packet); //invio risposta
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(SEND_ERR);
                }

            }

            //Chiudo le risorse.
            //socket.close();
        }
    }

    private class DSRowSwapHandler implements Runnable {

        private final DiscoveryServer reference;

        private final Thread myThread;

        private final int port;
        private final DatagramSocket socket;
        private final DatagramPacket packet;
        private final byte buf[] = new byte[256];

        public DSRowSwapHandler(DiscoveryServer reference, int rowSwapPort) throws SocketException {
            this.reference = reference;
            this.myThread = new Thread(this);
            this.port = rowSwapPort;
            this.socket = new DatagramSocket(rowSwapPort);
            this.packet = new DatagramPacket(buf, 0, buf.length);
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

        public int getPort() {
            return port;
        }

        @Override
        public void run() {
            //preparo strutture per lettura/scrittura dati
            String request = null;
            int response = RESULT_OK;

            while (true) {
                packet.setData(buf, 0, buf.length); //devo risettare ciclicamente il buffer del pacchetto

                try {
                    //mi pongo in attesa di un packet da parte di un client
                    socket.receive(packet);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(RECEIVE_ERR);
                }

                try (ByteArrayInputStream biStream = new ByteArrayInputStream(packet.getData()); DataInputStream diStream = new DataInputStream(biStream)) {
                    //Leggo la richiesta
                    request = diStream.readUTF();
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(DECODE_REQUEST_ERR);
                }

                //Decodifico la richiesta, la eseguo e preparo una risposta.
                StringTokenizer tokenizer = new StringTokenizer(request, ":");
                //Resetto la risposta.
                response = RESULT_OK;


                try {
                    String cmd = tokenizer.nextToken();
                    String filename = tokenizer.nextToken();
                    String address = tokenizer.nextToken();
                    int port = Integer.parseInt(tokenizer.nextToken());
                    InetAddressPortWrapper wrapper = new InetAddressPortWrapper(address, port);


                    //Parsing
                    if (cmd.equalsIgnoreCase(CMD_REGISTER)) {
                        //Verifico la disponibilità del filename
                        if (!reference.isFilenameInUse(filename)) {
                            //Verifico la disponibilità della porta.
                            if (!reference.isAddressPortInUse(wrapper)) {
                                //Allora posso registrare il row swap server
                                reference.putFilenamePortPair(filename, wrapper);
                                System.out.println("Registrato: " + filename + "; " + wrapper);
                            } else {
                                response = RESULT_PAIR_IN_USE;
                            }
                        } else {
                            response = RESULT_FILENAME_IN_USE;
                        }
                    } else if (cmd.equalsIgnoreCase(CMD_DISMISS)) {
                        //Verifico la disponibilità del filename.
                        if (reference.isFilenameInUse(filename)) {
                            //Verifico che filename e porta coincidino
                            if (reference.getAddressPortByFilename(filename).equals(wrapper)) {
                                //Allora posso cancellare il row swap server
                                reference.removeFilenamePortPair(filename);
                                System.out.println("Cancellato: " + filename + "; " + wrapper);
                            } else {
                                response = RESULT_PAIR_NOT_CONSISTENT;
                            }
                        } else {
                            response = RESULT_FILENAME_NOT_IN_USE;
                        }
                    } else {
                        response = RESULT_UNKNOWN_COMMAND;
                    }
                } catch (NoSuchElementException | NumberFormatException | UnknownHostException e) {
                    response = RESULT_MALFORMED_REQUEST;
                }

                try (ByteArrayOutputStream boStream = new ByteArrayOutputStream(); DataOutputStream doStream = new DataOutputStream(boStream)) {
                    doStream.writeInt(response);
                    //setto il contenuto della risposta
                    packet.setData(boStream.toByteArray());
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(ENCODE_RESPONSE_ERR);
                }

                try {
                    socket.send(packet); //invio risposta
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(SEND_ERR);
                }
            }
        }
    }

    //Immutable Object
    private class InetAddressPortWrapper {
        private InetAddress address;
        private int port;

        public InetAddressPortWrapper(String address, int port) throws UnknownHostException {
            this(InetAddress.getByName(address), port);
        }

        public InetAddressPortWrapper(InetAddress address, int port) {
            this.address = address;
            this.port = port;
        }

        public InetAddress getAddress() {
            return address;
        }

        public int getPort() {
            return port;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof InetAddressPortWrapper) {
                InetAddressPortWrapper other = (InetAddressPortWrapper) obj;

                return getAddress().equals(other.getAddress()) && getPort() == other.getPort();
            }
            return false;
        }

        @Override
        public String toString() {
            return "(" + address + ":" + port + ')';
        }
    }

    private final HashMap<String, InetAddressPortWrapper> mappaAddressPortRowSwapServer = new HashMap<>();

    private final DSClientHandler clientHandler;
    private final DSRowSwapHandler rowSwapHandler;


    public DiscoveryServer(int clientPort, int rowSwapPort) throws SocketException {
        this.clientHandler = new DSClientHandler(this, clientPort);
        this.rowSwapHandler = new DSRowSwapHandler(this, rowSwapPort);
    }

    public void start() {
        clientHandler.start();
        rowSwapHandler.start();
    }

    public void join() throws InterruptedException {
        clientHandler.join();
        rowSwapHandler.join();
    }

    public void putFilenamePortPair(String filename, InetAddressPortWrapper wrapper) {
        if (isPortValid(wrapper.getPort())) {
            synchronized (mappaAddressPortRowSwapServer) {
                mappaAddressPortRowSwapServer.put(filename, wrapper);
            }
        }
    }

    public void removeFilenamePortPair(String filename) {
        synchronized (mappaAddressPortRowSwapServer) {
            mappaAddressPortRowSwapServer.remove(filename);
        }
    }

    public InetAddressPortWrapper getAddressPortByFilename(String filename) {
        InetAddressPortWrapper wrapper;

        synchronized (mappaAddressPortRowSwapServer) {
            wrapper = mappaAddressPortRowSwapServer.getOrDefault(filename, null);
        }

        return wrapper;
    }

    public boolean isFilenameInUse(String filename) {
        boolean result;
        synchronized (mappaAddressPortRowSwapServer) {
            result = mappaAddressPortRowSwapServer.containsKey(filename);
        }
        return result;
    }

    public boolean isAddressPortInUse(InetAddressPortWrapper wrapper) {
        boolean result;
        synchronized (mappaAddressPortRowSwapServer) {
            result = mappaAddressPortRowSwapServer.containsValue(wrapper);
        }
        return result;
    }

    public int getClientPort() {
        return clientHandler.getPort();
    }

    public int getRowSwapPort() {
        return rowSwapHandler.getPort();
    }

    public static void main(String[] args) {

        //DiscoveryServer portaRichiesteClient portaRegistrazioneRS

        //controllo che l'utente abbia inserito portaDS e almeno un file e una porta
        if (args.length != 2) {
            System.out.println("Usage: DiscoveryServer portaRichiesteClient portaRegistrazioneRS");
            System.exit(INV_ERR);
        }

        //controllo porta DiscoveryServer
        int clientPort = -1;

        try {
            clientPort = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.out.println("Invalid client port: must be int 1024 < port < 64k");
            System.exit(INVALID_DS_PORT);
        }

        //se la porta del discovery server è out of range errore
        if (!isPortValid(clientPort)) {
            System.out.println("Invalid client port: must be int 1024 < port < 64k");
            System.exit(INVALID_DS_PORT);
        }

        int rowSwapPort = -1;

        try {
            rowSwapPort = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.out.println("Invalid port: must be int 1024 < port < 64k");
            System.exit(INVALID_DS_PORT);
        }

        //se la porta del discovery server è out of range errore
        if (!isPortValid(rowSwapPort)) {
            System.out.println("Invalid port: must be int 1024 < port < 64k");
            System.exit(INVALID_DS_PORT);
        }

        if (rowSwapPort == clientPort) {
            System.out.println("client port == row swap port");
            System.exit(INV_ERR);
        }

        DiscoveryServer server = null;

        try {
            server = new DiscoveryServer(clientPort, rowSwapPort);
            server.start();
        } catch (SocketException e) {
            e.printStackTrace();
            System.exit(SOCKET_ERR);
        }

        System.out.println("Server avviato.");
        System.out.println("Porta richieste clienti: " + server.getClientPort());
        System.out.println("Porta richieste row swap: " + server.getRowSwapPort());
        System.out.println("Attendo terminazione dei figli...");

        try {
            server.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(JOIN_ERR);
        }

        //Prevedere un sistema di uscita del DS, con segnalazione ai RowSwapRegistrati che il DS termina?

    }

}