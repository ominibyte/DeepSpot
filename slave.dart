import "dart:async";
import 'dart:convert';
import 'dart:io';

import 'util.dart';

final PORT = 50356;
var appId = "demo123";
bool instanceTermination = false;
bool jobCompleted = false;
Socket socket;

void main(List<String> args) async{
  // Retrieve the Application ID
  if( args.isNotEmpty && args.length >= 1 )
    appId = args[0];

  // check that the job has not been cancelled or finished yet
  Map map = getJobDetails(appId);

  if( map == null )
    return;

  if( map['status']["S"] == "FINISHED" || map['status']["S"] == "CANCELLED" ){
    cancelAndTerminateSpotInstance(map['sir']["S"]);
    return;
  }

  //TODO Check if this application saved a checkpoint and has not completed running. Download properties
  

  // change the status to running
  updateJobEntry(appId, "status", "RUNNING");

  // if this is not the first run, increment the restarts count in the database
  if( map["checkpoint"]["S"] != "" )
    updateJobEntry(appId, "restarts", "${int.parse(map["restarts"]["S"]) + 1}");

  // Start the socket connection to the server
  socketConnection();

  Future.any(<Future>[instanceTerminationCheck(), jobCompletionCheck()]).then((future){
    if( instanceTermination ){
      //TODO inform master and backup
      socket.write(jsonEncode({"type": "terminated"}));
    }
    else{ // Job completed
      //TODO inform master and save model to S3
      socket.write(jsonEncode({"type": "completed"}));
    }
  }).catchError((err){
    print(err);
  });
}

void socketConnection() async{
  // retrieve the serverIP from the database server
  ProcessResult results = Process.runSync('aws', ['dynamodb', 'scan', '--table-name', 'utilities', 
      '--filter-expression', '#ST = :a', 
      '--expression-attribute-names', '{"#ST": "id"}',
      '--expression-attribute-values', '{":a":{"S":"serverIP"}}'
  ]);
  Map map = jsonDecode(results.stdout);
  final obj = map["Items"][0];
  final serverIP = obj["value"]["S"];

  await Socket.connect(serverIP, PORT).then((s) => socket = s);
  
  // Introduce self to server with appId
  socket.writeln(jsonEncode({"type": "intro", "appId": appId}));

  socket.listen((raw){
    final data = utf8.decode(raw);

    //TODO check if there is a command to terminate
  });
}

Future instanceTerminationCheck() async{
  while( !instanceTermination ){
    await Future.delayed(Duration(seconds: 5));

    // check if the instance has been set to terminate
    ProcessResult result = Process.runSync("curl", ['-H', 'X-aws-ec2-metadata-token: curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600"', 
      '-v', 'http://169.254.169.254/latest/meta-data/spot/instance-action']);

    if( !(result.exitCode != 0 || result.stderr.toString().contains("404")) ){
      Map map = jsonDecode(result.stdout);
      if( map.containsKey("action") ){
        //{"action": "stop", "time": "2017-09-18T08:22:00Z"}
        //{"action": "terminate", "time": "2017-09-18T08:22:00Z"}
        instanceTermination = true;
        break;
      }
    }
  }

  return true;
}

Future jobCompletionCheck() async{
  while( !jobCompleted ){
    await Future.delayed(Duration(seconds: 5));

    //TODO check the properties file to know if the job has been completed

  }

  //TODO Save final model to S3

  return true;
}

Map getJobDetails(String jobId){
  ProcessResult results = Process.runSync('aws', ['dynamodb', 'scan', '--table-name', 'jobs', 
      '--filter-expression', '#ST = :a', 
      '--expression-attribute-names', '{"#ST": "id"}',
      '--expression-attribute-values', '{":a":{"S":"$jobId"}}']);
  
  Map map = jsonDecode(results.stdout);
  
  return map["Items"].isEmpty ? null : map["Items"][0];
}

cancelAndTerminateSpotInstance(String sir){
  // Cancel the instance request incase it has not been assigned
  cancelSpotInstanceRequest(sir);

  // Terminate the instance
  String instanceId = sirToSpotInstanceId(sir); // Get the instance ID
  if( instanceId == null )
    return;

  terminateInstance(instanceId);
}