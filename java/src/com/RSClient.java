package com;

import java.io.*;
import java.net.*;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

public class RSClient {

    private static final String DEFAULT_serverIP = "127.0.0.1";
    private static final int DEFAULT_serverPort = 6666;
    private static final int DEFAULT_bufferSize = 256;
    
    private static final int PARAM_ERR = 1;
    private static final int HOST_ERR = 2;
    private static final int NETW_ERR = 3;
    private static final int SERVICE_ERR = 4;
    private static final int EOF_OCC = 5;

    private static boolean isPortValid(int port) {
        return 0x400 < port && port <= 0xFFFF;
    }

    private final InetAddress addressDS;
    private final int portDS;

    private InetAddress addressRS = null;
    private int portRS = -1;

    private boolean networkState = false;

    private DatagramSocket socket;
    private DatagramPacket packet;

    private byte[] emptyBuffer = new byte[DEFAULT_bufferSize];

    public RSClient() throws UnknownHostException {
        this(DEFAULT_serverIP, DEFAULT_serverPort);
    }

    public RSClient(String serverIP, int portDS) throws UnknownHostException, IllegalArgumentException {
        this(InetAddress.getByName(serverIP), portDS);
    }

    public RSClient(InetAddress addressDS, int portDS) throws IllegalArgumentException {
        this.addressDS = addressDS;

        //Controllo che la porta sia non standard e nel range di 16-bit.
        if (!(isPortValid(portDS))) {
            throw new IllegalArgumentException("Porta non valida");
        }

        this.portDS = portDS;
    }

    public InetAddress getAddressDS() {
        return addressDS;
    }

    public int getPortDS() {
        return portDS;
    }

    public InetAddress getAddressRS() {
        return addressRS;
    }

    public int getPortRS() {
        return portRS;
    }

    /**
     * Inizializza gli oggetti di rete.
     *
     * @throws SocketException Non è stato possibile creare la socket
     */
    public void initNetwork() throws SocketException {
        socket = new DatagramSocket();
        //Se voglio imposto il timeout:
        //socket.setSoTimeout(DEFAULT_timeout);
        packet = new DatagramPacket(emptyBuffer, 0, emptyBuffer.length, addressDS, portDS);
        networkState = true;
    }

    /**
     * Richede al discovery server il servizio collegato al file.
     *
     * @param filename nome del file in qui fare lo swap
     * @return se non c'è errore ritorno null.
     */
    public String requestService(String filename) throws IOException, IllegalArgumentException {
        //Controllo argomenti
        if (filename.isBlank()) {
            throw new IllegalArgumentException("Filename vuoto");
        }

        //Devo verificare che la rete sia inizializzata.
        if (!networkState) throw new IllegalStateException("Bisogna inizializzare la rete prima");

        //Imposto il pacchetto volto al Discovery.
        packet.setAddress(addressDS);
        packet.setPort(portDS);

        //Imposto la richiesta.
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
            try (DataOutputStream dataStream = new DataOutputStream(byteStream)) {
                //Creo il messaggio per il discovery e lo salvo nel pacchetto.
                dataStream.writeUTF(filename);
                packet.setData(byteStream.toByteArray());
            }
        }

        //Invio la richiesta.
        socket.send(packet);

        //Attendo risposta.
        packet.setData(emptyBuffer);
        socket.receive(packet);

        //Decodifico la risposta:
        try (ByteArrayInputStream byteStream = new ByteArrayInputStream(packet.getData())) {
            try (DataInputStream dataStream = new DataInputStream(byteStream)) {
                //Ricavo la porta del servizio.
                String tmpString = dataStream.readUTF();
                StringTokenizer tokenizer = new StringTokenizer(tmpString, ":");

                try{
                    this.addressRS = InetAddress.getByName(tokenizer.nextToken());
                    this.portRS = Integer.parseInt(tokenizer.nextToken());
                }catch (UnknownHostException | NoSuchElementException | NumberFormatException e){
                    return tmpString;
                }

                //Verifico che la porta sia valida.
                //Controllo che la porta sia non standard e nel range di 16-bit.
                //Se il nome file non fosse fra quelli noti al DiscoveryServer, il
                //DiscoveryServer invia esito negativo e il client termina.

                //Controllo non necessario.
//                if (!(isPortValid(tmpPort)))
//                    return "Porta non valida";
//
//                this.portRS = tmpPort;


                return null;
            }
        }
    }


    /**
     * Chiede al server di swappare due righe.
     *
     * @param line1 linea 1 da swappare
     * @param line2 linea 2 da swappare
     * @return Stringa con l'esito del server.
     * @throws IOException Errore dovuto alla socket, stream
     */
    public String swapLines(int line1, int line2) throws IOException {
        //Controllo che le linee siano valide
        if (line1 < 0 || line2 < 0) {
            throw new IllegalArgumentException("Linee inserite non valide (< 0)");
        }

        //Linee uguali non devo richiedere il server.
        if (line1 == line2) {
            return "Esito POSITIVO (Local Check)";
        }

        //Devo verificare che la rete sia inizializzata.
        if (!networkState) throw new IllegalStateException("Bisogna inizializzare la rete prima");

        //Devo verificare che sia già stato trovato il servizio.
        if (!isPortValid(portRS)) throw new IllegalStateException("Bisogna cercare il servizio prima");

        //Ora posso chidere al servizio di swappare le righe:

        //Imposto la richiesta.
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
            try (DataOutputStream dataStream = new DataOutputStream(byteStream)) {
                //Creo il messaggio per il discovery e lo salvo nel pacchetto.
                dataStream.writeUTF(line1 + "," + line2);
                packet.setData(byteStream.toByteArray());
            }
        }

        //Invio la richiesta
        packet.setAddress(addressRS);
        packet.setPort(portRS);
        socket.send(packet);

        //Attendo risposta.
        packet.setData(emptyBuffer);
        socket.receive(packet);

        //Decodifico la risposta:
        try (ByteArrayInputStream byteStream = new ByteArrayInputStream(packet.getData())) {
            try (DataInputStream dataStream = new DataInputStream(byteStream)) {
                //Ricavo il risultato e lo mostro.
                String result = dataStream.readUTF();
                return result;
            }
        }

    }

    public static void main(String[] args) {
        //RSClient IPDS portDS fileName

        //Controllo argomenti inline
        if (args.length != 3) {
            System.out.println("RSClient IPDS portDS fileName");
            System.exit(PARAM_ERR);
        }

        //Parsing degli argomenti
        String serverIP = args[0];
        int serverPort = -1;
        try {
        	serverPort = Integer.parseInt(args[1]);
        } catch(NumberFormatException e) {
        	e.printStackTrace();
        	System.exit(PARAM_ERR);
        }
        
        String filename = args[2];

        //Controllo delgli arogmenti
        //La porta viene comunque controllata. Ma la ricontrolliamo...
        if (!isPortValid(serverPort)) {
            System.err.println("Porta server non valida");
            System.exit(PARAM_ERR);
        }

        RSClient client = null;
        String esitoServizio = null;

        //Creo l'oggetto client.
        try {
            client = new RSClient(serverIP, serverPort);
        } catch (UnknownHostException e) {
            System.err.println("Server ip non valido");
            System.exit(HOST_ERR);
        }

        //Mi connetto al server, richiedo il servizio e avvio REPL.
        try {
            client.initNetwork();
        } catch (SocketException e) {
            System.err.println("Impossibile inizializzare la rete: " + e.getLocalizedMessage());
            System.exit(NETW_ERR);
        }

        System.out.println("Rete inizializzata: " + serverIP + ":" + serverPort);

        try {
            esitoServizio = client.requestService(filename);
        } catch (IOException e) {
            System.err.println("Impossibile richiedere il servizio relativo a: " + filename);
            System.exit(SERVICE_ERR);
        }

        //se va tutto bene torno null quindi se non sono null errore
        if(esitoServizio != null){
            System.err.println(esitoServizio);
            System.exit(SERVICE_ERR);
        }

        System.out.println("Servizio trovato: " + client.getPortRS());

        //REPL while

        //Roba per il REPL
        int line1 = -1, line2 = -1;
        String tmpString = null;
        BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));

        try {
            while (true) {
                //Chiedo le linee da swappare:

                //LINEA 1
                System.out.println("Linea 1: ");
                tmpString = stdIn.readLine();

                if(tmpString == null) //EOF --> devo uscire (termino)
                    System.exit(EOF_OCC);

                try{
                    line1 = Integer.parseInt(tmpString) - 1;
                }catch (NumberFormatException ex){
                    System.out.println("Linea 1 malformata");
                    continue;
                }

                //LINEA 2
                System.out.println("Linea 2: ");
                tmpString = stdIn.readLine();

                if(tmpString == null) //se EOF termino
                	System.exit(EOF_OCC);

                try{
                    line2 = Integer.parseInt(tmpString) - 1;
                }catch (NumberFormatException ex){
                    System.out.println("Linea 2 malformata");
                    continue;//Nuovo ciclo REPL.
                }

                //Controllo delle linee.
                if(line1 < 0 | line2 < 0){
                    System.out.println("Linee non valide (<0)");
                    continue;//Nuovo ciclo REPL.
                }

                //Posso fare lo swapping.
                String esito = client.swapLines(line1, line2);

                System.out.println(esito);
            }
        } catch (IOException e) {
            System.err.println("Errore nella REPL: " + e.getLocalizedMessage());
            System.exit(SERVICE_ERR);
        }

    }

}
