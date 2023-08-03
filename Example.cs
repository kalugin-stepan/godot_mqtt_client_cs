using Godot;
using System.Text;

public partial class Example : Node {
	MQTT mqtt;
	double t = 0;
	public override void _Ready() {
		mqtt = GetParent().GetNode<MQTT>("MQTT");
		mqtt.SetLastWill("last", "BB");
		mqtt.BrokerConnected += () => {
			GD.Print("Connected");
			mqtt.Subscribe("test_sub");
		};
		mqtt.BrokerConnectionFailed += () => {
			GD.Print("Connection failed");
		};
		mqtt.BrokerDisconnected += () => {
			GD.Print("Disconnected");
		};
		mqtt.User = "user1";
		mqtt.Pswd = "123";
		mqtt.ReceivedMessage += (topic, data) => { // (string, byte[])
			GD.Print($"Received message '{Encoding.UTF8.GetString(data)}' from '{topic}'");
		};
		mqtt.ConnectToBroker("broker.emqx.io");
	}
	public override void _Process(double delta) {
		t += delta;
		if (t > 2 && mqtt.Connected) {
			mqtt.Publish("test_pub", "hello");
			t = 0;
		}
	}
}
