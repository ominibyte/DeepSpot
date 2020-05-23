
import 'dart:convert';
import 'dart:io';

Socket socket;
final PORT = 50356;
var appId = "testapp123";
var serverIP = "35.182.202.42";

void main() async{
  await Socket.connect(serverIP, PORT).then((s) => socket = s);

  if( socket != null )
    print("Connected to server");
  
  // Introduce self to server with appId
  socket?.writeln(jsonEncode({"type": "intro", "appId": appId}));

  socket?.listen((raw){
    final map = jsonDecode(utf8.decode(raw));

    if( map["type"] == "action" && map["action"] == "terminate" ){
      // Stop script if it still running
      // stopRunningScript();

      // // Backup model
      // backupModel();

      socket?.writeln(jsonEncode({"type": "terminated", "appId": appId}));
    }
  });
}