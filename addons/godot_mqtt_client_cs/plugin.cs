#if TOOLS
using Godot;

[Tool]
public partial class plugin : EditorPlugin {
	public override void _EnterTree() {
		var script = ResourceLoader.Load<Script>("res://addons/godot_mqtt_client_cs/MQTT.cs");
		var icon = ResourceLoader.Load<Texture2D>("res://addons/godot_mqtt_client_cs/icon.png");
		AddCustomType("MQTT", "Node", script, icon);
	}
	public override void _ExitTree() {
		RemoveCustomType("MQTT");
	}
}
#endif
