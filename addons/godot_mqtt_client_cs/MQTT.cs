using Godot;
using System;
using System.Text;

public partial class MQTT : Node {
	public bool Connected {
		get => brokerConnectMode == BCM_CONNECTED;
	}

	[Export]
	public string ClientId = "";
	[Export]
	public int VerboseLevel = 2;
	[Export]
	public ulong PingInterval = 30;

	StreamPeerTcp socket = null;
	StreamPeerTls sslSocket = null;
	WebSocketPeer webSocket = null;

	const int BCM_NOCONNECTION = 0;
	const int BCM_WAITING_WEBSOCKET_CONNECTION = 1;
	const int BCM_WAITING_SOCKET_CONNECTION = 2;
	const int BCM_WAITING_SSL_SOCKET_CONNECTION = 3;
	const int BCM_FAILED_CONNECTION = 5;
	const int BCM_WAITING_CONNMESSAGE = 10;
	const int BCM_WAITING_CONNACK = 19;
	const int BCM_CONNECTED = 20;

	int brokerConnectMode = BCM_NOCONNECTION;

	readonly RegEx regexBrokerUrl = new RegEx();

	const int DEFAULTBROKERPORT_TCP = 1883;
	const int DEFAULTBROKERPORT_SSL = 8886;
	const int DEFAULTBROKERPORT_WS = 8080;
	const int DEFAULTBROKERPORT_WSS = 8081;

	const byte CP_PINGREQ = 0xc0;
	const byte CP_PINGRESP = 0xd0;
	const byte CP_CONNACK = 0x20;
	const byte CP_CONNECT = 0x10;
	const byte CP_PUBLISH = 0x30;
	const byte CP_SUBSCRIBE = 0x82;
	const byte CP_UNSUBSCRIBE = 0xa2;
	const byte CP_PUBREC = 0x40;
	const byte CP_SUBACK = 0x90;
	const byte CP_UNSUBACK = 0xb0;

	UInt16 pid = 0;
	public string User = null;
	public string Pswd = null;
	public int KeepAlive = 120;
	string lwTopic = null;
	byte[] lwMsg = null;
	byte lwQos = 0;
	bool lwRetain = false;
	public event Action<string, byte[]> ReceivedMessage;
	public event Action BrokerConnected;
	public event Action BrokerDisconnected;
	public event Action BrokerConnectionFailed;
	public event Action<int> PublishAcknowledge;
	byte[] receivedBuffer = new byte[8];
	int receivedBufferLength = 0;
	string commonName = null;
	void AppendData(byte[] data) {
		if (receivedBufferLength + data.Length > receivedBuffer.Length) {
			var buf = new byte[receivedBuffer.Length + data.Length + 8];
			receivedBuffer.CopyTo(buf, 0);
			receivedBuffer = buf;
		}
		data.CopyTo(receivedBuffer, receivedBufferLength);
		receivedBufferLength += data.Length;
	}
	void SendData(byte[] data) {
		Error e = 0;
		if (sslSocket != null) {
			e = sslSocket.PutData(data);
		}
		else if (socket != null) {
			e = socket.PutData(data);
		}
		else if (webSocket != null) {
			e = webSocket.PutPacket(data);
		}
		if (e != 0) {
			GD.PrintT("Error: ", e);
		}
	}
	void ReceiveIntoBuffer() {
		if (sslSocket != null) {
			var sslSocketStatus = sslSocket.GetStatus();
			if (sslSocketStatus == StreamPeerTls.Status.Connected || sslSocketStatus == StreamPeerTls.Status.Handshaking) {
				sslSocket.Poll();
				int n = sslSocket.GetAvailableBytes();
				if (n != 0) {
					var sv = sslSocket.GetData(n);
					if (sv[0].AsInt32() != 0) {
						throw new Exception($"Error code: {sv[0].AsInt32()}");
					}
					AppendData(sv[1].AsByteArray());
				}
			}
		}
		else if (socket != null && socket.GetStatus() == StreamPeerTcp.Status.Connected) {
			socket.Poll();
			int n = socket.GetAvailableBytes();
			if (n == -1) {
				GD.Print("Fuck");
			}
			if (n != 0) {
				var sv = socket.GetData(n);
				if (sv[0].AsInt32() != 0) {
					throw new Exception($"Error code: {sv[0].AsInt32()}");
				}
				AppendData(sv[1].AsByteArray());
			}
		}
		else if (webSocket != null) {
			webSocket.Poll();
			while (webSocket.GetAvailablePacketCount() != 0) {
				AppendData(webSocket.GetPacket());
			}
		}
	}
	ulong pingTicksNext0 = 0;
	public override void _Process(double delta) {
		if (brokerConnectMode == BCM_NOCONNECTION) {}
		else if (brokerConnectMode == BCM_WAITING_WEBSOCKET_CONNECTION) {
			webSocket.Poll();
			var webSocketState = webSocket.GetReadyState();
			if (webSocketState == WebSocketPeer.State.Closed) {
				if (VerboseLevel != 0) {
					GD.Print($"WebSocket closed with code: {webSocket.GetCloseCode()}, reason {webSocket.GetCloseReason()}.");
				}
				brokerConnectMode = BCM_FAILED_CONNECTION;
				if (BrokerConnectionFailed != null) BrokerConnectionFailed.Invoke();
			}
			else if (webSocketState == WebSocketPeer.State.Open) {
				brokerConnectMode = BCM_WAITING_CONNMESSAGE;
				if (VerboseLevel != 0) {
					GD.Print("Websocket connection now open");
				}
			}
		}
		else if (brokerConnectMode == BCM_WAITING_SOCKET_CONNECTION) {
			socket.Poll();
			var socketStatus = socket.GetStatus();
			if (socketStatus == StreamPeerTcp.Status.Error) {
				if (VerboseLevel != 0) {
					GD.PrintErr("TCP socket error");
				}
				brokerConnectMode = BCM_FAILED_CONNECTION;
				if (BrokerConnectionFailed != null) BrokerConnectionFailed.Invoke();
			}
			if (socketStatus == StreamPeerTcp.Status.Connected) {
				brokerConnectMode = BCM_WAITING_CONNMESSAGE;
			}
		}
		else if (brokerConnectMode == BCM_WAITING_SSL_SOCKET_CONNECTION) {
			socket.Poll();
			var socketStatus = socket.GetStatus();
			if (socketStatus == StreamPeerTcp.Status.Error) {
				if (VerboseLevel != 0) {
					GD.Print("TCP socket error before SSL");
				}
				brokerConnectMode = BCM_FAILED_CONNECTION;
				if (BrokerConnectionFailed != null) BrokerConnectionFailed.Invoke();
			}
			if (socketStatus == StreamPeerTcp.Status.Connected) {
				if (sslSocket == null) {
					sslSocket = new StreamPeerTls();
					if (VerboseLevel != 0) {
						GD.Print($"Connecting socket to SSL with common_name={commonName}");
					}
					Error E3 = sslSocket.ConnectToStream(socket, commonName);
					if (E3 != 0) {
						GD.PrintErr($"bad sslsocket.connect_to_stream E={E3}");
						brokerConnectMode = BCM_FAILED_CONNECTION;
						if (BrokerConnectionFailed != null) BrokerConnectionFailed.Invoke();
						sslSocket = null;
					}
				}
				if (sslSocket != null) {
					sslSocket.Poll();
					var sslSocketStatus = sslSocket.GetStatus();
					if (sslSocketStatus == StreamPeerTls.Status.Connected) {
						brokerConnectMode = BCM_WAITING_CONNMESSAGE;
					}
					else if (sslSocketStatus >= StreamPeerTls.Status.Error) {
						GD.PrintErr("bad sslsocket.connect_to_stream");
						if (BrokerConnectionFailed != null) BrokerConnectionFailed.Invoke();
					}
				}
			}
		}
		else if (brokerConnectMode == BCM_WAITING_CONNMESSAGE) {
			SendData(FirstMessageToServer());
			brokerConnectMode = BCM_WAITING_CONNACK;
		}
		else if (brokerConnectMode == BCM_WAITING_CONNACK || brokerConnectMode == BCM_CONNECTED) {
			ReceiveIntoBuffer();
			WaitMsg();
			if (brokerConnectMode == BCM_CONNECTED && pingTicksNext0 < Time.GetTicksMsec()) {
				PingReq();
				pingTicksNext0 = Time.GetTicksMsec() + PingInterval*1000;
			}
		}
		else if (brokerConnectMode == BCM_FAILED_CONNECTION) {
			CleanupSockets();
		}
	}
	public override void _Ready() {
		regexBrokerUrl.Compile("^(tcp://|wss://|ws://|ssl://)?([^:\\s]+)(:\\d+)?(/\\S*)?$");
		if (ClientId == "") {
			ClientId = $"rr{Random.Shared.Next(10000)}";
		}
	}
	public void SetLastWill(string topic, byte[] msg, bool retain = false, byte qos = 0) {
		if (qos > 2) throw new ArgumentException("qos should be < 3");
		if (topic == null) throw new ArgumentException("topic should't be null");
		lwTopic = topic;
		lwMsg = msg;
		lwRetain = retain;
		lwQos = qos;
		if (VerboseLevel != 0) {
			string x = retain ? "retain" : "";
			GD.Print($"LASTWILL{x} topic={topic} msg={msg}");
		}
	}
	public void SetLastWill(string topic, string msg, bool retain = false, byte qos = 0) {
		SetLastWill(topic, Encoding.UTF8.GetBytes(msg), retain, qos);
	}
	byte[] FirstMessageToServer() {
		bool cleanSession = true;

		int l = 14 + ClientId.Length
		+ (lwTopic != null ? 4 + lwTopic.Length + lwMsg.Length : 0)
		+ (User != null ? 4 + User.Length + Pswd.Length : 0);

		byte[] msg = new byte[l];

		msg[0] = CP_CONNECT;
		msg[1] = (byte)(10 + 2 + ClientId.Length);
		msg[2] = 0x00;
		msg[3] = 0x04;
		const string mqtt = "MQTT";
		for (int j = 0; j < mqtt.Length; j++) {
			msg[4 + j] = (byte)mqtt[j];
		}
		msg[8] = 0x04;
		msg[9] = (byte)(cleanSession ? (1<<1) : 0);
		msg[10] = 0x00;
		msg[11] = 0x3C;

		msg[12] = (byte)(ClientId.Length >> 8);
		msg[13] = (byte)(ClientId.Length & 0xFF);
		for (int j = 0; j < ClientId.Length; j++) {
			msg[14 + j] = (byte)ClientId[j];
		}
		int i = 14 + ClientId.Length;
		if (KeepAlive != 0) {
			if (KeepAlive > 65535) throw new Exception("keepalive should be < 65536");
			msg[10] |= (byte)(KeepAlive >> 8);
			msg[11] |= (byte)(KeepAlive & 0x00FF);
		}
		if (lwTopic != null) {
			msg[1] += (byte)(2 + lwTopic.Length + 2 + lwMsg.Length);
			msg[9] |= (byte)(0x4 | (lwQos & 0x1) << 3 | (lwQos & 0x2) << 3);
			msg[9] |= (byte)(lwRetain ? 1<<5 : 0);
			msg[i] = (byte)(lwTopic.Length >> 8);
			i++;
			msg[i] = (byte)(lwTopic.Length & 0xFF);
			i++;
			foreach (char j in lwTopic) {
				msg[i] = (byte)j;
				i++;
			}
			msg[i] = (byte)(lwMsg.Length >> 8);
			i++;
			msg[i] = (byte)(lwMsg.Length & 0xFF);
			i++;
			lwMsg.CopyTo(msg, i);
			i += lwMsg.Length;
		}
		if (User != null) {
			msg[1] += (byte)(2 + User.Length + 2 + Pswd.Length);
			msg[9] |= 0xC0;
			msg[i] = (byte)(User.Length >> 8);
			i++;
			msg[i] = (byte)(User.Length & 0xFF);
			i++;
			foreach (char j in User) {
				msg[i] = (byte)j;
				i++;
			}
			msg[i] = (byte)(Pswd.Length >> 8);
			msg[i] = (byte)(Pswd.Length & 0xFF);
			foreach (char j in Pswd) {
				msg[i] = (byte)j;
			}
		}
		return msg;
	}
	bool CleanupSockets(bool retval = false) {
		if (VerboseLevel != 0) {
			GD.Print("CleanupSockets");
		}
		if (socket != null) {
			if (sslSocket != null) sslSocket = null;
			socket.DisconnectFromHost();
			socket = null;
		}
		else {
			if (sslSocket != null) throw new Exception("wtf");
		}
		if (webSocket != null) {
			webSocket.Close();
			webSocket = null;
		}
		brokerConnectMode = BCM_NOCONNECTION;
		return retval;
	}
	public bool ConnectToBroker(string brokerUrl) {
		if (brokerConnectMode != BCM_NOCONNECTION) throw new Exception("Broker already connected");
		var brokerMatch = regexBrokerUrl.Search(brokerUrl);
		if (brokerMatch == null) {
			GD.PrintErr($"ERROR: unrecognized brokerurl pattern: {brokerUrl}");
			return CleanupSockets();
		}
		string[] brokerComponents = brokerMatch.Strings;
		string brokerProtocol = brokerComponents[1];
		string brokerServer = brokerComponents[2];
		bool isWebSocket = brokerProtocol == "ws://" || brokerProtocol == "wss://";
		bool isSsl = brokerProtocol == "ssl://" || brokerProtocol == "wss://";
		int brokerPort = isWebSocket ? (isSsl ? DEFAULTBROKERPORT_WSS : DEFAULTBROKERPORT_WS) : isSsl ? DEFAULTBROKERPORT_SSL : DEFAULTBROKERPORT_TCP;
		int n;
		if (brokerComponents.Length > 3 && int.TryParse(brokerComponents[4], out n)) {
			brokerPort = n;
		}
		string brokerPath = brokerComponents.Length > 4 ? brokerComponents[4] : "";

		commonName = null;

		if (isWebSocket) {
			webSocket = new WebSocketPeer();
			webSocket.SupportedProtocols = new string[] {"mqttv3.1"};
			string webSocketUrl = (isSsl ? "wss://" : "ws://") + brokerServer + ":" + brokerPort.ToString() + brokerPath;
			if (VerboseLevel != 0) {
				GD.Print($"Connecting to websocketurl: {webSocketUrl}");
				Error E = webSocket.ConnectToUrl(webSocketUrl);
				if (E != 0) {
					GD.PrintErr($"ERROR: websocketclient.connect_to_url Err: {E}");
					return CleanupSockets();
				}
			}
			GD.Print($"Websocket get_requested_url {webSocket.GetRequestedUrl()}");
			brokerConnectMode = BCM_WAITING_WEBSOCKET_CONNECTION;
		}
		else {
			socket = new StreamPeerTcp();
			if (VerboseLevel != 0) {
				GD.Print($"Connecting to {brokerServer}:{brokerPort}");
			}
			Error E = socket.ConnectToHost(brokerServer, brokerPort);
			if (E != 0) {
				GD.PrintErr($"ERROR: socketclient.connect_to_url Err: {E}");
				return CleanupSockets();
			}
			if (isSsl) {
				brokerConnectMode = BCM_WAITING_SSL_SOCKET_CONNECTION;
				commonName = brokerServer;
			}
			else {
				brokerConnectMode = BCM_WAITING_SOCKET_CONNECTION;
			}
		}
		return true;
	}
	public void DisconnectFromServer() {
		if (brokerConnectMode == BCM_CONNECTED) {
			SendData(new byte[] {0xE0, 0x00});
			if (BrokerDisconnected != null) BrokerDisconnected.Invoke();
		}
		CleanupSockets();
	}
	public int Publish(string topic, byte[] msg, bool retain=false, byte qos=0) {
		int sz = 2 + topic.Length + msg.Length + (qos > 0 ? 2 : 0);
		if (sz > 2097151) throw new Exception("sz should be < 2097152");

		int l = 3 + topic.Length + msg.Length + sz / 127 + (sz % 127 != 0 ? 1 : 0) + (qos > 0 ? 2 : 0);
		byte[] pkt = new byte[l];
		pkt[0] = (byte)(CP_PUBLISH | (qos != 0 ? 2 : 0) | (retain ? 1 : 0));
		int i = 1;
		while (sz > 0x7f) {
			pkt[i] = (byte)((sz & 0x7f) | 0x80);
			sz >>= 7;
			i++;
		}
		pkt[i] = (byte)sz;
		i++;
		pkt[i] = (byte)(topic.Length >> 8);
		i++;
		pkt[i] = (byte)(topic.Length & 0xFF);
		i++;
		foreach (char j in topic) {
			pkt[i] = (byte)j;
			i++;
		}
		if (qos > 0) {
			if (pid == UInt16.MaxValue) pid = 0;
			else pid++;
			pkt[i] = (byte)(pid >> 8);
			i++;
			pkt[i] = (byte)(pid & 0xFF);
			i++;
		}
		msg.CopyTo(pkt, i);
		SendData(pkt);
		if (VerboseLevel >= 2) {
			string a = qos != 0 ? $"[{pid}]" : "";
			string b = retain ? " <retain>" : "";
			GD.Print($"CP_PUBLISHP{a}{b} topic={topic} msg={Encoding.UTF8.GetString(msg)}");
		}
		return pid;
	}
	public int Publish(string topic, string msg, bool retain=false, byte qos=0) {
		return Publish(topic, Encoding.UTF8.GetBytes(msg), retain, qos);
	}
	public void Subscribe(string topic, byte qos = 0) {
		if (pid == UInt16.MaxValue) pid = 0;
		else pid++;
		int lenght = 2 + 2 + topic.Length + 1;
		int l = 7 + topic.Length;
		byte[] msg = new byte[l];
		msg[0] = CP_SUBSCRIBE;
		msg[1] = (byte)(lenght);
		msg[2] = (byte)(pid >> 8);
		msg[3] = (byte)(pid & 0xFF);
		msg[4] = (byte)(topic.Length >> 8);
		msg[5] = (byte)(topic.Length & 0xFF);
		for (int i = 0; i < topic.Length; i++) {
			msg[6 + i] = (byte)topic[i];
		}
		msg[6 + topic.Length] = qos;
		if (VerboseLevel != 0) {
			GD.Print($"SUBSCRIBE[{pid}] topic={topic}");
		}
		SendData(msg);
	}
	void PingReq() {
		if (VerboseLevel > 1) {
			GD.Print("PINGREQ");
		}
		SendData(new byte[]{CP_PINGREQ, 0x00});
	}
	public void Unsubscribe(string topic) {
		if (pid == UInt16.MaxValue) pid = 0;
		else pid++;
		int lenght = 2 + 2 + topic.Length;
		int l = 6 + topic.Length;
		byte[] msg = new byte[l];
		msg[0] = CP_UNSUBSCRIBE;
		msg[1] = (byte)lenght;
		msg[2] = (byte)(pid >> 8);
		msg[3] = (byte)(pid & 0xFF);
		msg[4] = (byte)(topic.Length >> 8);
		msg[5] = (byte)(topic.Length & 0xFF);
		for (int i = 0; i < topic.Length; i++) {
			msg[6 + i] = (byte)topic[i];
		}
		if (VerboseLevel != 0) {
			GD.Print($"UNSUBSCRIBE[{pid}] topic={topic}");
		}
		SendData(msg);
	}
	Error WaitMsg() {
		int n = receivedBufferLength;
		if (n < 2) return 0;
		byte op = receivedBuffer[0];
		int i = 1;
		int sz = receivedBuffer[i] & 0x7f;
		while ((receivedBuffer[i] & 0x80) != 0) {
			i++;
			if (i == n) return 0;
			sz += (receivedBuffer[i] & 0x7f) << ((i-1)*7);
		}
		i++;
		if (n < i + sz) {
			return 0;
		}
		Error E = 0;
		if (op == CP_PINGRESP) {
			if (sz != 0) throw new ArgumentException("wtf2");
			if (VerboseLevel > 1) {
				GD.Print("PINGRESP");
			}
		}
		else if ((op & 0xF0) == 0x30) {
			int topic_len = (receivedBuffer[i]<<8) + receivedBuffer[i+1];
			int im = i + 2;
			char[] topic = new char[topic_len];
			for (int j = 0; j < topic_len; j++) {
				topic[j] = (char)receivedBuffer[im + j];
			}
			im += topic_len;
			int pid1 = 0;
			if ((op & 6) != 0) {
				pid1 = (receivedBuffer[im]<<8) + receivedBuffer[im+1];
				im += 2;
			}
			int data_len = i + sz - im;
			byte[] data = new byte[data_len];

			for (int j = 0; j < i + sz - im; j++) {
				data[j] = receivedBuffer[im + j];
			}
			if (VerboseLevel > 1) {
				GD.Print($"received topic={topic} msg={Encoding.UTF8.GetString(data)}");
			}
			if (ReceivedMessage != null) ReceivedMessage.Invoke(new string(topic), data);

			if ((op & 6) == 2) {
				SendData(new byte[] {0x40, 0x02, (byte)(pid1 >> 8), (byte)(pid1 & 0xFF)});
			}
			else if ((op & 6) == 4) {
				throw new Exception("wtf3");
			}
		}
		else if (op == CP_CONNACK) {
			if (sz != 2) throw new Exception("wtf4");
			byte retCode = receivedBuffer[i+1];
			if (VerboseLevel != 0) {
				GD.Print($"CONNACK ret={retCode}02x");
			}
			if (retCode == 0x00) {
				brokerConnectMode = BCM_CONNECTED;
				if (BrokerConnected != null) BrokerConnected.Invoke();
			}
			else {
				if (VerboseLevel != 0) {
					GD.Print($"Bad connection retcode={retCode}");
				}
				if (BrokerConnectionFailed != null) BrokerConnectionFailed.Invoke();
				E = Error.Failed;
			}
		}
		else if (op == CP_PUBREC) {
			if (sz != 2) throw new Exception("wtf5");
			int apid = (receivedBuffer[i]<<8) + receivedBuffer[i+1];
			if (VerboseLevel > 1) {
				GD.Print($"PUBACK[{apid}]");
			}
			if (PublishAcknowledge != null) PublishAcknowledge.Invoke(apid);
		}
		else if (op == CP_SUBACK) {
			if (sz != 3) throw new Exception("wtf6");
			int apid = (receivedBuffer[i]<<8) + receivedBuffer[i+1];
			if (VerboseLevel != 0) {
				GD.Print($"SUBACK[{apid}] ret={receivedBuffer[i+2]}02x");
			}
			if (receivedBuffer[i+2] == 0x80) {
				E = Error.Failed;
			}
		}
		else if (op == CP_UNSUBACK) {
			if (sz != 2) throw new Exception("wtf7");
			int apid = (receivedBuffer[i]<<8) + receivedBuffer[i+1];
			if (VerboseLevel != 0) {
				GD.Print($"UNSUBACK[{apid}]");
			}
		}
		else if (VerboseLevel != 0) {
			GD.PrintErr($"Unknown MQTT opcode op={op}");
		}
		TrimReceivedBuffer(i + sz);
		return E;
	}
	void TrimReceivedBuffer(int n) {
		if (n == receivedBufferLength) {
			receivedBufferLength = 0;
		}
		else {
			if (n > receivedBufferLength) throw new Exception("how");
			for (int i = 0; i < receivedBufferLength - n; i++) {
				receivedBuffer[i] = receivedBuffer[n + i];
			}
			receivedBufferLength -= n;
		}
	}
}
