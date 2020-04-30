import "dart:async";
import 'dart:convert';
import 'dart:io';

final PORT = 50356;

Map<String, Socket> socketMap = {};

main(){
  ServerSocket.bind('127.0.0.1', PORT)
  .then((serverSocket) {
    serverSocket.listen((socket) {
      socket.listen((raw){
        Map dataMap = jsonDecode(utf8.decode(raw));
        
        switch(dataMap["type"]){
          case "intro": socketMap[dataMap["appId"]] = socket; break;
          case "terminated": //TODO break;
          case "completed": //TODO break;
        }
      });
    });
  });
}