import "dart:async";
import 'dart:convert';
import 'dart:io';
import 'util.dart';

final PORT = 50356;

Map<String, Socket> socketMap = {};

final python = "python3.7";

main(){
  ProcessResult result = Process.runSync('curl', ['http://169.254.169.254/latest/meta-data/public-ipv4']);
  print("Public IP: " + result.stdout);

  final publicIP = result.stdout;

  // Update the IP address of the master
  Process.runSync('aws', ['dynamodb', 'update-item', '--table-name', 'utilities', '--key', 
    '{"id": {"S":"serverIP"}}', '--update-expression', 'SET #ST = :val',
    '--expression-attribute-names', '{"#ST": "value"}',
    '--expression-attribute-values', '{":val":{"S":"$publicIP"}}'
  ]);

  ServerSocket.bind('127.0.0.1', PORT)
  .then((serverSocket) {
    serverSocket.listen((socket) {
      print("Connected to ${socket.remoteAddress.rawAddress}");

      socket.listen((raw){
        Map dataMap = jsonDecode(utf8.decode(raw));
        
        switch(dataMap["type"]){
          case "intro": socketMap[dataMap["appId"]] = socket; 
            print("Handshake received: " + dataMap["appId"]);
            break;
          case "terminated": // This is is in response to a command from the master to terminate
            // Terminate the spot instance
            cancelAndTerminateSpotInstance(dataMap["sir"]);
            print("Terminated: " + dataMap["appId"]);
            break;
          case "completed": 
            // Terminate the spot instance
            cancelAndTerminateSpotInstance(dataMap["sir"]);
            print("Completed: " + dataMap["appId"]);
            break;
          case "time":
            // Terminate the spot instance
            cancelAndTerminateSpotInstance(dataMap["sir"]);
            print("Time Elapsed: " + dataMap["appId"]);
            relaunchJob(dataMap["appId"]);
            break;
          case "instance-termination": 
            // Terminate the spot instance
            cancelAndTerminateSpotInstance(dataMap["sir"]);
            print("Instance Recalled: " + dataMap["appId"]);
            relaunchJob(dataMap["appId"]);
            break;
          case "test": print(dataMap["payload"]); break;
        }
      });
    });
  });
}

// Relaunch job on a new instance
relaunchJob(id) async{
  // Get the last 7 days price
    final now = DateTime.now().toUtc();
    Future<List> spotPricesFuture = getSpotPrices(now.subtract(Duration(days: 7)), now);

    // Get the current on demand prices
    final loadFuture = loadOnDemandPricing();

    // Save the data to json file.
    List spotPrices = await spotPricesFuture;
    // Delete the input and output files
    File jsonFile = new File("sample.json");
    if( jsonFile.existsSync() )
      jsonFile.deleteSync();
    jsonFile = new File("expectTime.json");
    if( jsonFile.existsSync() )
      jsonFile.deleteSync();

    jsonFile = new File("sample.json");
    
    // Write the input file data
    jsonFile.writeAsStringSync(jsonEncode(spotPrices));

    // Ask the model to predict
    ProcessResult result = Process.runSync(python, ["predict.py", jsonFile.absolute.toString()]);
    if( result.stderr != null && result.stderr.toString().isNotEmpty ){
      printError(result.stderr);
      return;
    }

    File responseFile = new File("expectTime.json");  // Read the out file data from the prediction script
    if( !responseFile.existsSync() ){
      printError("Unable to find the predicted response file: " + result.stdout);
      return;
    }

    List modelResponseList = jsonDecode(responseFile.readAsStringSync());

    await loadFuture; // ensure that the pricing CSV is loaded

    // Determine the specs and OS which will be used for hosting the app
    Map<String, dynamic> instanceMap = findOptimalInstance(modelResponseList, getCurrentSpotPrices(spotPrices));
    
    if( instanceMap == null ){
      printError("Sorry, unable to find a spot instance to service this request at this moment.");
      return;
    }
    
    //os, region, instanceType, availabilityZone, id, price
    final instance = {
      "os": "amazon-linux-2", //TODO randomly choose OS. For now lets just use amazon-linux which comes with awscli
      "region": instanceMap["AvailabilityZone"].toString().substring(0, instanceMap["AvailabilityZone"].toString().length - 1),
      "instanceType": instanceMap["InstanceType"],
      "availabilityZone": instanceMap["AvailabilityZone"],
      "price": instanceMap["price"],
      "id": id,
      "time": instanceMap["time"],
    };


    final interruptTime = DateTime.now().add(Duration(minutes: int.parse(instance['time'].toString()))).toUtc().toIso8601String();

    // Update entry in database
    Process.runSync('aws', ['dynamodb', 'update-item', '--table-name', 'jobs', '--key', 
      '{"id": {"S":"$id"}}', '--update-expression', 'SET location = :val2, instance = :val3, estimatedInterrupt = :val4, bidPrice = :val5, interruptMinutes = :val6, spotPrice = :val7',
      '--expression-attribute-values', '{":val2":{"S":"${instance['region']}"},":val3":{"S":"${instance['instanceType']}"},":val4":{"S":"$interruptTime"},":val5":{"S":"${instance['price']}"},":val6":{"S":"${instance['time']}"},":val7":{"S":"${instanceMap['SpotPrice']}"}}'
    ]);

    Map<String, dynamic> response = requestSpotInstance(instance);
    
    if( response["status"] ){  
      print("Job submitted!!!");
      print(response);


      // check if the spot instance was reserved
      String state = response["payload"]["SpotInstanceRequests"][0]["State"];
      String sir = response["payload"]["SpotInstanceRequests"][0]["SpotInstanceRequestId"];
      if( state == "open" || state == "active" ){
        // Update the job database entry with the Spot Instance Request ID
        Process.runSync('aws', ['dynamodb', 'update-item', '--table-name', 'jobs', '--key', 
          '{"id": {"S":"$id"}}', '--update-expression', 'SET sir = :val',
          '--expression-attribute-values', '{":val":{"S":"$sir"}}'
        ]);

        return;
      }
    }

    printError(response["error"]);

    // delete the uploaded files from S3
    Process.runSync('aws', ['s3', 'rm', "s3://comp598-deepspot/$id", '--recursive']);
    
    // remove the entry from the database
    Process.runSync('aws', ['dynamodb', 'put-item', '--table-name', 'jobs', '--key', 
      '{"id": {"S":"$id"}}'
    ]);
}

printError(error){
  print(error);
}