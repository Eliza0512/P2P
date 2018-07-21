import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

///  Z5089071 ASSIGNMENT FOR COMP9331
public class cdht {
	static String id, s1, s2;
	static int terminated=0;
	ArrayList<String> predecessors= new ArrayList<String>();
	public cdht(String id,String s1,String s2) {
		this.id=id;
		this.s1=s1;
		this.s2=s2;
		//System.out.println("Success initialize Peer "+id +" with successor "+s1+" and "+s2);
	}
	
	///////////////  STEP 2   SEND PING REQUEST ////////////////////////
	public class PingClient extends Thread{
		@Override
		public void run() {
			while(cdht.terminated<2) {
				(new PingSend(s1)).start();
				(new PingSend(s2)).start();
				try {
					Thread.sleep(60*1000);// send Ping Request every 60 seconds
				}catch(InterruptedException e) {
					continue;
				}
			}
		}
	}
	// send PingRequest thread
	public class PingSend extends Thread{
		String successor;
		public PingSend(String s) {
			this.successor=s;
		}
		@Override
		public void run() {
			sendPing();
		}
		
		public void sendPing(){
			int failTime=0;
			while(failTime<3) { // timeout more than 3 times means this successor has left
				try {
					DatagramSocket clientSocket = new DatagramSocket();
					clientSocket.setSoTimeout(10*1000); // if no connection try again 10 seconds later
					InetAddress IPAddress = InetAddress.getByName("localhost");
					byte[] sendData = new byte[1024];
					byte[] receiveData = new byte[1024];
					sendData = cdht.id.getBytes();
					DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, 60000+Integer.parseInt(successor)); // successor as server to receive request
					clientSocket.send(sendPacket);
					DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
					clientSocket.receive(receivePacket);
					String p = new String(receivePacket.getData());
					System.out.println("A ping response message was received from Peer " + successor+".");
					clientSocket.close();
					break;
				}catch(Exception e){
					//System.out.println("Send Ping request fail #"+successor);
					failTime+=1;
					continue;
				}
			}
			if(failTime == 3) {
				System.out.println("Peer "+successor+" is no longer alive.");
				killed(successor);
			}
		}
		////////////////////////////////////////////////////
		
		
		
		/////////////////      STEP 5    /////////////////////
		// if one of its successors is killed 
		// send TCP request ask for new successors
		public void killed(String s) {
			if(s.equals(cdht.s1)) { // first successor left, then ask second successors's first successor
				cdht.s1=new String(cdht.s2);
				try {
					InetAddress IPAddress = InetAddress.getByName("localhost");
					Socket clientSocket = new Socket(IPAddress, 60000+Integer.parseInt(cdht.s2));
					DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
					BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
					outToServer.writeBytes("successor 1" + '\n');
					String responseSentence = inFromServer.readLine();
					if(responseSentence.startsWith("newSuccessor")) {
						String newS = responseSentence.split(" ")[1];
						cdht.s2=newS;
					}
					clientSocket.close();	
				}catch(Exception e) {
					System.out.println(e.getMessage());
				}
				System.out.println("My first successor is now peer "+s1+".");
				System.out.println("My second successor is now peer "+s2+".");
			}else if(s.equals(cdht.s2)) { // second successor left, then ask first successors's second successor
				try {
					InetAddress IPAddress = InetAddress.getByName("localhost");
					Socket clientSocket = new Socket(IPAddress, 60000+Integer.parseInt(cdht.s1));
					DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
					BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
					outToServer.writeBytes("successor 2" + '\n');
					String responseSentence = inFromServer.readLine();
					if(responseSentence.startsWith("newSuccessor")) {
						String newS = responseSentence.split(" ")[1];
						cdht.s2=newS;
					}
					clientSocket.close();	
				}catch(Exception e) {
					System.out.println(e.getMessage());
				}
				System.out.println("My first successor is now peer "+s1+".");
				System.out.println("My second successor is now peer "+s2+".");
			}
		}
		////////////////////////////////////////////////////
	}
	///////////// STEP2 RECEIVE PING AND SEND PING RESPONSE//////////////////
	// receive PingRequest from predecessor
	public class PingServer extends Thread{
		@Override
		public void run() {
			try {
				DatagramSocket serverSocket = new DatagramSocket(60000+Integer.parseInt(id));  // self as server to receive request		
				while(cdht.terminated<2){
					byte[] receiveData = new byte[1024];
					byte[] sendData = new byte[1024];
					DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
					serverSocket.receive(receivePacket);
					String sentence = new String( receivePacket.getData());
					System.out.println("A ping request message was received from Peer " + sentence+".");
					// update predecessors set
					if(!predecessors.contains(sentence.trim())) {
						if(predecessors.size()==2) {
							predecessors.clear();
						}
						predecessors.add(sentence.trim());
					}
					InetAddress IPAddress = receivePacket.getAddress();
					int port = receivePacket.getPort();
					sendData = id.getBytes();
					DatagramPacket sendPacket =
							new DatagramPacket(sendData, sendData.length, IPAddress, port);
					serverSocket.send(sendPacket);
	           }
			}catch(Exception e) {
				//System.out.println("Receive ping message error");
				System.out.println(e.getMessage());
			}
		}
		
	}
	//////////////////////////////////////////////////////////////
	
	// processing user input, each input command is an individual thread
	public class TCPClient extends Thread{
		@Override
		public void run() {
			Scanner reader = new Scanner(System.in);  // Reading from System.in
			while(cdht.terminated<2) {
				String request = reader.nextLine().toLowerCase();
				RequestSender rs = new RequestSender(request);
				rs.start();
			}
			reader.close();
			//System.out.println("TCP Client stop");
		}
	}
	public class TCPServer extends Thread{
		@Override
		public void run() {
			String clientSentence;
			try {
				ServerSocket welcomeSocket = new ServerSocket(60000+Integer.parseInt(id));
				while (cdht.terminated<2) {
					Socket connectionSocket = welcomeSocket.accept();
					BufferedReader inFromClient =
					    new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
					DataOutputStream outToClient = new DataOutputStream(connectionSocket.getOutputStream());
					clientSentence = inFromClient.readLine();
					if(clientSentence.trim()==null) {
						continue;
					}
					if(clientSentence.startsWith("file")) {  // pass file request to successors
						String[] fileInfo = clientSentence.trim().split(" ");
						handleFile(Integer.parseInt(fileInfo[1]),fileInfo[3],fileInfo[5]);
					}else if(clientSentence.startsWith("Received a response message")) {  // receive location of request file
						System.out.println(clientSentence);
					}else if(clientSentence.startsWith("departure")) {      // departure msg  from successor
						String[] departureInfo = clientSentence.trim().split(" ");
						handleDeparture(departureInfo[1],departureInfo[3],departureInfo[4]);
					}else if(clientSentence.equals("confirm departure msg")) {  // ack for previous sent departure msg
						cdht.terminated += 1;
					}else if(clientSentence.startsWith("successor")) {
						String s_num = clientSentence.split(" ")[1];
						if(s_num.equals("1")) { // ask for first successor
							outToClient.writeBytes("newSuccessor "+cdht.s1+"\n");
						}else {   //ask fro second successor
							outToClient.writeBytes("newSuccessor "+cdht.s2+"\n");
						}
					}
					if(terminated==2) {
						break;
					}
				}
				if(terminated==2) {
					welcomeSocket.close();
				}
			}catch(Exception e) {
				System.out.println(e.getMessage());
			}
			
		}
		////////////// STEP 3  HANDLE REQUEST FILE FROM PREDESECCORS/////////////////////
		public void  handleFile(int n,String source,String prev) {
			int location = n%256;
			if(location==0) {location=256;}
			int locationSelf = Integer.parseInt(id);
			int locationPrev = Integer.parseInt(prev);
			if((locationSelf>=location && locationPrev < location) || 
					(locationSelf < location && locationPrev>locationSelf && location>locationPrev)) { // this peer stored this file
				System.out.println("File "+String.valueOf(n)+" is here");
				String responseMsg = "Received a response message from peer " + id+", which has the file " + String.valueOf(n)+ ".";
				try {
					InetAddress IPAddress = InetAddress.getByName("localhost");
					Socket clientSocket = new Socket(IPAddress, 60000+Integer.parseInt(source)); // send TCP request to successor
					DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
					outToServer.writeBytes(responseMsg + '\n');  // only need to pass request, no need for response
					clientSocket.close();
				}catch(Exception e) {
					//System.out.println("file response send error!");
					System.out.println(e.getMessage());
				}
				System.out.println("A response message, destined for peer "+source+", has been sent.");
			}else { // send file request to its successor
				System.out.println("File "+String.valueOf(n)+" is not stored here");
				// pass this request to next Peer
				try {
					InetAddress IPAddress = InetAddress.getByName("localhost");
					String requestNext = "file "+String.valueOf(n)+" source "+source+" previous "+id;
					Socket clientSocket = new Socket(IPAddress, 60000+Integer.parseInt(s1)); // send TCP request to successor
					DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
					outToServer.writeBytes(requestNext + '\n');  // only need to pass request, no need for response
					clientSocket.close();
					System.out.println("File request message has been forwarded to my successor.");
				}catch(Exception e) {
					//System.out.println("request file send error!");
					System.out.println(e.getMessage());
				}
			}
		}
		////////////////////////////////////////////////////////////
		
		/////////////STEP4 PROCESSING DEPARTURE MSG FROM SUCCESSORS//////////////////
		// method to handle departure msg from sucessors
		public void handleDeparture(String s, String newSOne, String newSTwo){
			// update successors
			if(s.equals(s1)) {  
				s1 = newSOne;
				s2 = newSTwo;
			}else {
				s2 = newSOne;
			}
			System.out.println("Peer "+s+" will depart from the network.");
			System.out.println("My first successor is now peer "+s1+".");
			System.out.println("My second successor is now peer "+s2+".");
			// send confirmation of this departure Msg
			try {
				InetAddress IPAddress = InetAddress.getByName("localhost");
				Socket clientSocket = new Socket(IPAddress, 60000+Integer.parseInt(s));
				DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
				outToServer.writeBytes("confirm departure msg");
				clientSocket.close();
			}catch(Exception e) {
				//System.out.println("confirm deaprture error!");
				System.out.println(e.getMessage());
			}
		}
		///////////////////////////////////////////////////
	}
	public class RequestSender extends Thread{
		String request;
		public RequestSender(String s) {
			this.request=s;
		}
		@Override
		public void run() {
			if(request.startsWith("request")) {
				int n = Integer.parseInt(request.trim().split(" ")[1]);
				requestFile(n,id);
			}else if(request.startsWith("quit")) {
				quit();
			}	
		}
		////////// STEP3 SEND FILE REQUEST///////////////
		public void requestFile(int n,String source) {
			String requestNext = "file "+String.valueOf(n)+" source "+source +" previous "+ source;
			System.out.println("File request message for "+String.valueOf(n)+" has been forwarded to my successor.");
			try {
				InetAddress IPAddress = InetAddress.getByName("localhost");
				Socket clientSocket = new Socket(IPAddress, 60000+Integer.parseInt(s1)); // send TCP request
				DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
				BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
				outToServer.writeBytes(requestNext + '\n');
				//String modifiedSentence = inFromServer.readLine();
				//System.out.println(modifiedSentence);
				clientSocket.close();
			}catch(Exception e) {
				//System.out.println("request file send error!");
				System.out.println(e.getMessage());
			}
		}
		/////////////////////////////////////////////
		
		
		///////////// STEP 4   SEND DEPARTURE /////////////////
		public void quit() {
			String departureMsg = "departure "+id+" successors "+s1+" "+s2;
			try {
				InetAddress IPAddress = InetAddress.getByName("localhost");
				Socket clientSocket = new Socket(IPAddress, 60000+Integer.parseInt(predecessors.get(0).trim())); // send departure Msg to first predecessors
				DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
				outToServer.writeBytes(departureMsg + '\n');
				clientSocket.close();
				// build connection to second predecessors
				Socket clientSocket2 = new Socket(IPAddress, 60000+Integer.parseInt(predecessors.get(1).trim())); // send departure Msg to second predecessors
				DataOutputStream outToServer2 = new DataOutputStream(clientSocket2.getOutputStream());
				outToServer2.writeBytes(departureMsg + '\n');
				clientSocket2.close();
			}catch(Exception e) {
				//System.out.println("departure Msg send error!");
				System.out.println(e.getMessage());
			}
			
		}
		/////////////////////////////////////////////
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		cdht peer = new cdht(args[0],args[1],args[2]);
		PingServer ping_server = peer.new PingServer();
		PingClient ping_client = peer.new PingClient();
		TCPServer tcp_server = peer.new TCPServer();
		TCPClient tcp_client = peer.new TCPClient();
		tcp_server.start();
		tcp_client.start();
		ping_server.start();
		ping_client.start();
		
		
	}
	

}
