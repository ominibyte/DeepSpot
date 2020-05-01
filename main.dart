import 'dart:convert';

import 'package:args/command_runner.dart';
import 'package:dart_console/dart_console.dart';
import 'dart:io';
import 'package:args/args.dart';

import 'table.dart';
import 'util.dart';

final console = Console();
final runner = CommandRunner("dspot", "DeepSpot Manager command-line application");
final parser = ArgParser();
const prompt = 'dspot> ';
bool isInteractiveMode = false;
final python = "/Users/richboy/opt/anaconda3/bin/python3";

void main(List<String> arguments) async{
  //printJobs(getJobs(JobStatus.ALL));
  // if( arguments.isEmpty || (arguments.length == 1 && arguments[0] == "dspot") ){  // Interactive mode
  //   runner.addCommand(JobsCommand());
  //   runner.addCommand(JobCommand());
  //   runner.addCommand(MasterCommand());

  //   isInteractiveMode = true;
  //   interactiveMode();
  // }
  // else{ // Non-interactive mode
  //   runner.addCommand(NonInteractiveModeCommand());
  //   runCommand(arguments);
  // }

  await submitJob("testapp123", "TestApp", "script.py", "test1/data.npz", "test1/model.h5");


  // final client = HttpClient();
  // final req = await client.getUrl(Uri.parse("https://api.pricing.us-east-1.amazonaws.com"));
  // final resp = await req.close();
  // resp.listen((data){
  //   print(utf8.decode(data));
  // });
}

void runCommand(List<String> arguments){
  runner.run(arguments);
  // .catchError((error){
  //   if( error is! UsageException )
  //     throw error;
    
  //   print(error);

  //   exit(64);
  // });
}

void interactiveMode(){
  while (true) {
    console.setBackgroundColor(ConsoleColor.black);
    console.setForegroundColor(ConsoleColor.cyan);
    console.write(prompt);
    console.setForegroundColor(ConsoleColor.white);

    final response = console.readLine(cancelOnBreak: true);
    if (response == null || response.isEmpty) {
      exit(0);
    } 
    else {
      // process args
      runCommand(response.split(" "));
    }
  }
}

void returnOutput(String output){
  print(output);
}

Map getJobs(JobStatus status){
  ProcessResult results;

  var statusText = status.toString().split(".")[1].trim();
  
  if( status == JobStatus.ALL )
    results = Process.runSync('aws', ['dynamodb', 'scan', '--table-name', 'jobs']);
  else{
    results = Process.runSync('aws', ['dynamodb', 'scan', '--table-name', 'jobs', 
      '--filter-expression', '#ST = :a', 
      '--expression-attribute-names', '{"#ST": "status"}',
      '--expression-attribute-values', '{":a":{"S":"$statusText"}}']);
  }
      
  //print(results.stderr);
  return jsonDecode(results.stdout);
}

/// Print job details in a table
void printJobs(Map map){
  final list = map["Items"];

  if( list.isEmpty ){
    console.setForegroundColor(ConsoleColor.yellow);
    printOutput("No job found for criteria.");
    return;
  }

  CommandLineTable table = new CommandLineTable();
  
  for( Map entry in list )
    printJobTableDetails(table, entry);
  table.doPrint();
}

void printJobTableHeader(CommandLineTable table){
  final headers = ["ID", "Name", "Replication", "Location", "Instance", "Run Time", "Status"];
  table.setShowVerticalLines(true);
  table.setHeaders(headers);
}

void printJobTableDetails(CommandLineTable table, Map entry){
  final time = DateTime.now().difference(DateTime.parse(entry["startTime"]["S"]));
  final duration = new Duration(milliseconds: time.inMilliseconds);
  table.addRow([entry["id"]["S"], entry["name"]["S"], entry["repl"]["N"], entry["location"]["S"], entry["instance"]["S"], duration.toString(), entry["status"]["S"]]);
}

enum JobStatus{
  RUNNING, FINISHED, CANCELLED, INITIALIZING, ALL
}

class NonInteractiveModeCommand extends Command{
  @override
  String get description => "Root command";

  @override
  String get name => "dspot";

  NonInteractiveModeCommand(){
    addSubcommand(JobsCommand());
    addSubcommand(JobCommand());
    addSubcommand(MasterCommand());

    argParser.addFlag("interactive", abbr: "i", negatable: false);
  }

  run(){
    // if( argResults['help'] )
    //   runner.printUsage();
    // else 
    if( argResults['interactive'] )
      interactiveMode();
  }
}

class MasterCommand extends Command{
  @override
  String get description => "Command to reserve and terminate master instance";

  @override
  String get name => "master";
  
  MasterCommand(){
    addSubcommand(MasterStartCommand());
    addSubcommand(MasterStopCommand());
  }
}

class MasterStartCommand extends Command{
  @override
  String get description => "Start a new on-demand instance";

  @override
  String get name => "start";
  
  run(){
    // start
    ProcessResult result = Process.runSync('aws', ['ec2', 'run-instances', 
      '--image-id', 'ami-054362537f5132ce2',// 'ami-078044add7f3bc223',
      '--count', '1', 
      '--instance-type', 'm5.large',
      //'--client-token', masterClientToken,
      '--key-name', 'Default-Key-Pair',
      '--security-group-ids', 'sg-0caf14f9eccb80727', // 'sg-0c3c674870aedb77b',
      //'--network-interfaces', 'AssociatePublicIpAddress=true,DeleteOnTermination=true,DeviceIndex=0',
      '--user-data',  'file://master-launch.sh' //new File("master-launch.sh").readAsStringSync()
    ]);

    if( result.stderr != null && result.stderr.toString().isNotEmpty )
      printError(result.stderr);
    else{
      console.setForegroundColor(ConsoleColor.green);
      printOutput("Master instance is initializing.");
      printOutput("");
    }
  }
}

class MasterStopCommand extends Command{
  @override
  String get description => "Stop all on-demand instances";

  @override
  String get name => "stop";
  
  run(){
    // Get all the on-demand instance IDs
    final result = Process.runSync('aws', ['ec2', 'describe-instances', 
      '--filters', 'Name=instance-state-name,Values=running', //'Name=client-token,Values=$masterClientToken',
      '--query', 'Reservations[*].Instances[*].{ID:InstanceId}'
    ]);

    List list = jsonDecode(result.stdout)[0];
    final instanceIds = list.map((item) => item["ID"]).toList();

    // Terminate the instances
    Process.runSync('aws', ['ec2', 'terminate-instances', '--instance-ids', ...instanceIds]);
    
    console.setForegroundColor(ConsoleColor.green);
    printOutput("All master instances stopped.");
    printOutput("");
  }
}

class JobCommand extends Command{
  @override
  String get description => "See the details of job, cancel a job or retrieve the final model";

  @override
  String get name => "job";

  JobCommand(){
    argParser.addFlag("get-model", abbr: "g", negatable: false, help: "Download the final model.");
    argParser.addFlag("cancel", abbr: "c", negatable: false, help: "Cancel a running job. Simulates OS interuption of a job.");
  }

  run() async{
    final isGetModel = argResults["get-model"] ? true : false;
    final isCancel = argResults["cancel"] ? true : false;

    if( argResults.rest.isEmpty ){
      console.setForegroundColor(ConsoleColor.red);
      printOutput("Format: job " + (isGetModel ? "--get-model " : 
        (isCancel ? "--cancel " : "")) + "<job-id>"); 

      return;
    }

    final jobId = argResults.rest[0];


    Map map = getJobDetails(jobId);
    // Check job exists
    if( map == null ){
      printError("No job exists with the ID: $jobId");
      return;
    }

    if( argResults["get-model"] ){
      // Check if the job is completed
      if( map['status']["S"] != "FINISHED" ){
        printError("The job $jobId is still running.");
        return;
      }

      // Download the model
      await downloadModelFromS3(jobId);

      print("Model downloaded to: " + "$jobId-model.h5");
    }
    else if( argResults["cancel"] ){
      // Check that the job has not completed
      if( map['status']["S"] == "FINISHED" ){
        printError("The job $jobId has already completed.");
        return;
      }

      // cancel the job
      updateJobEntry(jobId, "status", "CANCELLED"); // Change the status on the database

      // Cancel the instance request incase it has not been assigned
      cancelSpotInstanceRequest(map["sir"]["S"]);

      // Terminate the instance
      String instanceId = sirToSpotInstanceId(map["sir"]["S"]); // Get the instance ID
      if( instanceId == null ){
        printError("Error discovered. Try again later.");
        return;
      }
      terminateInstance(instanceId);

      print("Job $jobId has been cancelled.");
    }
    else{
      // show the details of the job
      CommandLineTable table = new CommandLineTable();
      printJobTableDetails(table, map);
      table.doPrint();
    }
  }
}

class JobsCommand extends Command{
  @override
  String get description => "All jobs commands";

  @override
  String get name => "jobs";

  JobsCommand(){
    addSubcommand(ListAllJobsCommand());
    addSubcommand(ListStatusJobsCommand());
    addSubcommand(ListCompletedJobsCommand());
    addSubcommand(SubmitJobCommand());
  }
}

class ListStatusJobsCommand extends Command{
  @override
  String get description => "List jobs based on status";

  @override
  String get name => "list";

  ListStatusJobsCommand(){
    argParser
      ..addOption("status", abbr: "s", help: "The type status of jobs to list. Supported options are: ALL, FINISHED, RUNNING, CANCELLED, INITIALIZING", defaultsTo: "ALL");

      // allowed: [
      //   "ALL",
      //   "FINISHED",
      //   "COMPLETED",
      //   "RUNNING",
      //   "CANCELLED",
      //   "INITIALIZING"
      // ]
  }

  void run(){
    JobStatus status = JobStatus.ALL;

    switch( argResults["status"].toString().toLowerCase() ){
      case "finished":
      case "completed": status = JobStatus.FINISHED; break;
      case "running": status = JobStatus.RUNNING; break;
      case "cancelled": status = JobStatus.CANCELLED; break;
      case "initializing": status = JobStatus.INITIALIZING; break;
      case "all": status = JobStatus.ALL; break;
      default: 
        console.setForegroundColor(ConsoleColor.red);
        printOutput("Invalid Job Status: " + argResults["status"]); 
        return;
    }

    Map<String, dynamic> map = getJobs(status);

    if( isInteractiveMode ){
      printJobs(map);
    }
    else
      returnOutput(map["Items"].toString());
  }
} 

class ListAllJobsCommand extends Command{
  @override
  String get description => "List all jobs, running or not";

  @override
  String get name => "list-all";

  void run(){
    Map<String, dynamic> map = getJobs(JobStatus.ALL);

    if( isInteractiveMode ){
      // var list = [
      //   {"id": "hgwftdgbd663", "name": "Some Name", "repl": 1, "location": "us-east-1", "instance": "p2.x5medium", "startTime": DateTime(2020).toIso8601String(), "status": "RUNNING"},
      // ];
      printJobs(map);
    }
    else{
      //TODO transform data for proper output from Dynamo JSON to regular JSON
      returnOutput(map["Items"].toString());
    }
  }
} 

class ListCompletedJobsCommand extends Command{
  @override
  String get description => "List all completed jobs.";

  @override
  String get name => "list-completed";

  void run(){
    Map<String, dynamic> map = getJobs(JobStatus.FINISHED);

    if( isInteractiveMode ){
      printJobs(map);
    }
    else
      returnOutput(map["Items"].toString());
  }
} 

class SubmitJobCommand extends Command{
  @override
  String get description => "Submit a job";

  @override
  String get name => "submit";

  SubmitJobCommand(){
    argParser
      //..addOption("os", abbr: "o", help: "The operating system to use. Default is random.", defaultsTo: "random", allowed: ["random", "windows", "unix", "linux"])
      ..addOption("script", abbr: "s", help: "The python script file to execute")
      ..addOption("input", abbr: "i", help: "The input file with the filtered data which will be used to train the model")
      ..addOption("model", abbr: "m", help: "The starting model file")
      ..addOption("name", abbr: "n", help: "The name of the job [optional]. A random name will be generated if not provided.");
  }

  void printError(String message){
    console.setForegroundColor(ConsoleColor.red);
    if( isInteractiveMode )
      console.writeLine(message);
    else
      printOutput(message);
    console.setForegroundColor(ConsoleColor.white);
    if( isInteractiveMode )
      console.writeLine();
    else
      printOutput("");

    this.printUsage();
  }

  void run() async{
    // if( argResults['script'] == null ){
    //   printError("Missing required script option.");
    //   return;
    // }
    if( argResults['input'] == null ){
      printError("Missing required script option.");
      return;
    }
    if( argResults['model'] == null ){
      printError("Missing required script option.");
      return;
    }
    String id = "${(DateTime.now().millisecondsSinceEpoch / 1000)}";
    String jobName = "Job " + id;
    if( argResults['name'] != null ){
      jobName = argResults['name'];
    }
    String script = "script.py";//argResults['script'];
    String inputFile = argResults['input'];
    String model = argResults['model'];

    await submitJob(id, jobName, script, inputFile, model);
  }
} 

Future uploadToS3(String localFilePath, String appId, String s3Name){
  return Process.run('aws', ['s3', 'cp', localFilePath, "s3://comp598-deepspot/$appId/$s3Name"]);
}

Future downloadModelFromS3(String appId){
  return Process.run('aws', ['s3', 'cp', "s3://comp598-deepspot/$appId/model.h5", "$appId-model.h5"]);
}

printError(String message){
  if( isInteractiveMode ){
    console.setForegroundColor(ConsoleColor.red);
    console.writeErrorLine(message);
  }
  else
    print(message);
}

printOutput(String message){
  if( isInteractiveMode )
    console.writeLine(message);
  else
    print(message);
}

submitJob(id, jobName, script, inputFile, model) async{
  // check if the files exists
  if( FileSystemEntity.typeSync(script) == FileSystemEntityType.notFound ){
    printError("The specified file in 'script' does not exist.");
    return;
  }
  if( FileSystemEntity.typeSync(inputFile) == FileSystemEntityType.notFound ){
    printError("The specified file in 'input' does not exist.");
    return;
  }
  if( FileSystemEntity.typeSync(model) == FileSystemEntityType.notFound ){
    printError("The specified file in 'model' does not exist.");
    return;
  }

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

  //print(jsonFile.absolute);

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
    printError("Sorry, unable to find a spot instance to service your request at this moment.");
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

  // Upload the model, script and input to S3
  await Future.wait([
    uploadToS3(model, id, "model.h5"),
    uploadToS3(inputFile, id, "data.npz"),
    uploadToS3(script, id, "script.py"),
  ]);

  final interruptTime = DateTime.now().add(Duration(minutes: int.parse(instance['time'].toString()))).toUtc().toIso8601String();

  // save entry to database
  result = Process.runSync('aws', ['dynamodb', 'put-item', '--table-name', 'jobs', '--item', 
    '{"id": {"S":"$id"}, "name": {"S":"$jobName"}, "instance": {"S":"${instance['instanceType']}"},' 
    '"location":{"S":"${instance['region']}"}, "repl":{"N":"1"}, '
    '"startTime":{"S":"${DateTime.now().toUtc().toIso8601String()}"}, "status":{"S":"INITIALIZING"},'
    '"estimatedInterrupt":{"S":"$interruptTime"}, "restarts": {"S":"0"}, "operatingSystem":{"S":"${instance['os']}"},'
    '"bidPrice":{"S":"${instance['price']}"}, "interruptMinutes":{"S":"${instance['time']}"},'
    '"spotPrice":{"S":"${instanceMap['SpotPrice']}"}, "checkpoint": {"S":"NULL"}}'
  ]); // checkpoint here is the time of the last checkpoint. sir is the Spot instance request ID
  if( result.stderr != null && result.stderr.toString().isNotEmpty ){
    printError("${result.stderr}");
    return;
  }

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