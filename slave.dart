import "dart:async";
import 'dart:convert';
import 'dart:io';

import 'util.dart';

final PORT = 50356;
var appId = "testapp123";
bool instanceTermination = false;
bool jobCompleted = false;
bool predictionTimeElapsed = false;
Socket socket;
Process process;
bool quit = false;
Map jobDetails;
final python = "python";

void main(List<String> args) async{
  // Retrieve the Application ID
  if( args.isNotEmpty && args.length >= 1 )
    appId = args[0];

  // check that the job has not been cancelled or finished yet
  Map map = getJobDetails(appId);
  jobDetails = map;

  if( map == null )
    return;

  if( map['status']["S"] == "FINISHED" || map['status']["S"] == "CANCELLED" ){
    cancelAndTerminateSpotInstance(map['sir']["S"]);
    return;
  }

  // change the status to running
  updateJobEntry(appId, "status", "RUNNING");

  // if this is not the first run, increment the restarts count in the database
  if( map["checkpoint"]["S"] != "NULL" )
    updateJobEntry(appId, "restarts", "${int.parse(map["restarts"]["S"]) + 1}");

  // Start the socket connection to the server
  await socketConnection();

  Process.start(python, ['script.py']).then((pro) {
    print("Started programm in separate process.");
    stdout.addStream(pro.stdout);
    stderr.addStream(pro.stderr);
    process = pro;
  });

  await Future.any(<Future>[instanceTerminationCheck(), jobCompletionCheck(), 
    predictionTimeCompletionCheck((double.parse(map["interruptMinutes"]["S"]) * 60000).toInt())]
  ).then((future) async{
    quit = true;  // Stop sending heartbeat

    if( instanceTermination || predictionTimeElapsed ){
      stopRunningScript();  // Stop the running script

      await backupModel();  // Backup Model to S3

      // inform master 
      if( socket != null ){
        if( instanceTermination )
          socket.writeln(jsonEncode({"type": "instance-termination", "appId": appId, "sir": jobDetails['sir']["S"]}));
        else
          socket.writeln(jsonEncode({"type": "time", "appId": appId, "sir": jobDetails['sir']["S"]}));
      }
    }
    else{ // Job completed
      // change the status to finished
      updateJobEntry(appId, "status", "FINISHED");

      await backupModel();  // Backup Model to S3

      // inform master and save model to S3
      socket?.write(jsonEncode({"type": "completed", "appId": appId, "sir": jobDetails['sir']["S"]}));
    }
  });
  // .catchError((err){
  //   print(err);
  // });
}

bool stopRunningScript(){
  if( process != null )
    return process.kill();

  print("Stopped running script.");

  return false;
}

backupModel() async{
  // Update last checkpoint time
  updateJobEntry(appId, "checkpoint", "${DateTime.now().toUtc().toIso8601String()}");

  // Backup model to S3
  await Future.wait([
    uploadToS3("model.h5", appId, "model.h5"),
    if( new File("new_model.h5").existsSync() )
      uploadToS3("new_model.h5", appId, "new_model.h5"),
  ]);
}

Future uploadToS3(String localFilePath, String appId, String s3Name){
  return Process.run('aws', ['s3', 'cp', localFilePath, "s3://comp598-deepspot/$appId/$s3Name"]);
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
  print("Server IP: $serverIP");

  await Socket.connect(serverIP, PORT).then((s) => socket = s);

  if( socket != null )
    print("Connected to server");
  
  // Introduce self to server with appId
  socket?.writeln(jsonEncode({"type": "intro", "appId": appId}));

  // Start sending heartbeat message
  heartBeatMessage();

  test();

  socket?.listen((raw){
    try{
      final map = jsonDecode(utf8.decode(raw));

      if( map["type"] == "action" && map["action"] == "terminate" ){
        print("Terminate request received!");
        
        // Stop script if it still running
        stopRunningScript();

        // Backup model
        backupModel();

        socket?.writeln(jsonEncode({"type": "terminated", "appId": appId}));
      }
    }
    catch(e){
      //ignore
    }
  });
}

/// Just for testing the instance termination query works
test(){
  ProcessResult result = Process.runSync("curl", ['-H', 'X-aws-ec2-metadata-token: curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600"', 
      '-v', 'http://169.254.169.254/latest/meta-data/spot/instance-action']);
  if( result.stderr != null && result.stderr.toString().isNotEmpty )
    socket?.writeln(jsonEncode({"type": "test", "payload": result.stderr, "appId": appId}));
  if( result.stdout != null && result.stdout.toString().isNotEmpty )
    socket?.writeln(jsonEncode({"type": "test", "payload": result.stdout, "appId": appId}));
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

    // check if the job has been completed
    if( new File("_COMPLETED").existsSync() )
      break;
  }

  // save end time
  updateJobEntry(appId, "endTime", "${DateTime.now().toUtc().toIso8601String()}");

  jobCompleted = true;

  return true;
}

/// If the predicted time for running the job has elapsed
Future predictionTimeCompletionCheck(millis) async{
  await Future.delayed(Duration(milliseconds: millis));
  predictionTimeElapsed = true;
}

Map getJobDetails(String jobId){
  ProcessResult results = Process.runSync('aws', ['dynamodb', 'scan', '--table-name', 'jobs', 
      '--filter-expression', '#ST = :a', 
      '--expression-attribute-names', '{"#ST": "id"}',
      '--expression-attribute-values', '{":a":{"S":"$jobId"}}']);
  
  Map map = jsonDecode(results.stdout);
  
  return map["Items"].isEmpty ? null : map["Items"][0];
}

/// Send heartbeat message every 10 seconds to the master
heartBeatMessage() async{
  while( !quit ){
      await Future.delayed(Duration(seconds: 10));

      socket?.writeln(jsonEncode({"type": "heartbeat", "appId": appId}));
  }
}