import 'dart:convert';

import 'package:args/command_runner.dart';
import 'package:dart_console/dart_console.dart';
import 'dart:io';
import 'package:args/args.dart';
import 'package:csv/csv.dart';

import 'table.dart';
import 'util.dart';

final console = Console();
final runner = CommandRunner("dspot", "DeepSpot Manager command-line application");
final parser = ArgParser();
const prompt = 'dspot> ';
bool isInteractiveMode = false;
final modelScript = "prediction.py";

final instanceTypes = [
  "p2.xlarge", "p2.8xlarge", "p2.16xlarge",
  "p3.2xlarge", "p3.8xlarge", "p3.16xlarge", "p3dn.24xlarge"
];

final regions = [
  'ap-northeast-1', 'ap-northeast-2', 'ap-south-1', 'ap-southeast-1', 'ap-southeast-2',
  'ca-central-1', 'us-east-1', 'us-east-2', 'us-west-1', 'us-west-2'
];

final regionNamesMap = {
  "US East (Ohio)": "us-east-2",
  "US East (N. Virginia)": "us-east-1",
  "US West (N. California)": "us-west-1",
  "US West (Oregon)": "us-west-2",
  "Asia Pacific (Tokyo)": "ap-northeast-1",
  "Asia Pacific (Seoul)": "ap-northeast-2",
  "Asia Pacific (Mumbai)": "ap-south-1",
  "Canada (Central)": "ca-central-1",
  "Asia Pacific (Singapore)": "ap-southeast-1",
  "Asia Pacific (Sydney)": "ap-southeast-2"
};

final ami = {
  "ubuntu-18.04": {
    "us-east-1": "ami-0dbb717f493016a1a",
    "us-east-2": "ami-0d6808451e868a339",
    "ap-northeast-1": "ami-09cff0147f55f2eb2",
    "ap-northeast-2": "ami-047dc469d9a3bbc4f",
    "ap-south-1": "ami-067b6fcd1ef7156ed",        // This AMI does not seem to available here cause price is 0.
    "ap-southeast-1": "ami-0a7789c77135a5f8a",
    "ap-southeast-2": "ami-0454424b678cb7dea",
    "ca-central-1": "ami-0142046278ba71f25",
    "us-west-1": "ami-09f2f73141c83d4fe",
    "us-west-2": "ami-008d8ed4bd7dc2485"
  },
  "amazon-linux-2": {
    "us-east-1": "ami-03d7bb62671766e1e",
    "us-east-2": "ami-0c6bb3d47bb198095",
    "ap-northeast-1": "ami-09cff0147f55f2eb2",
    "ap-northeast-2": "ami-047dc469d9a3bbc4f",
    "ap-south-1": "ami-08804058ce83e33d8",        // This AMI does not seem to available here cause price is 0.
    "ap-southeast-1": "ami-0131cfff5655522af",
    "ap-southeast-2": "ami-0f86cfc4495d1ef9f",
    "ca-central-1": "ami-069916fe4d52f0cd7",
    "us-west-1": "ami-0b87e1ab3d1087b47",
    "us-west-2": "ami-0ea3e3316596177cc"
  }
};

Map<String, dynamic> pricing;

void main(List<String> arguments) async{
  //printJobs(getJobs(JobStatus.ALL));
  if( arguments.isEmpty || (arguments.length == 1 && arguments[0] == "dspot") ){  // Interactive mode
    runner.addCommand(JobsCommand());
    runner.addCommand(JobCommand());

    isInteractiveMode = true;
    interactiveMode();
  }
  else{ // Non-interactive mode
    runner.addCommand(NonInteractiveModeCommand());
    runCommand(arguments);
  }


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
  console.writeLine(output);
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
    console.writeLine("No job found for criteria.");
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
      console.writeLine("Format: job " + (isGetModel ? "--get-model " : 
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
        console.writeLine("Invalid Job Status: " + argResults["status"]); 
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
    else
      returnOutput(map["Items"].toString());
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
      ..addOption("os", abbr: "o", help: "The operating system to use. Default is random.", defaultsTo: "random", allowed: ["random", "windows", "unix", "linux"])
      ..addOption("script", abbr: "s", help: "The python script file to execute")
      ..addOption("input", abbr: "i", help: "The input file with the filtered data which will be used to train the model")
      ..addOption("model", abbr: "m", help: "The starting model file")
      ..addOption("name", abbr: "n", help: "The name of the job [optional]. A random name will be generated if not provided.");
  }

  void printError(String message){
    console.setForegroundColor(ConsoleColor.red);
    console.writeLine(message);
    console.setForegroundColor(ConsoleColor.white);
    console.writeLine();

    this.printUsage();
  }

  void run() async{
    if( argResults['script'] == null ){
      printError("Missing required script option.");
      return;
    }
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
    String script = argResults['script'];
    String inputFile = argResults['input'];
    String model = argResults['model'];

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
    
    // Write the input file data
    jsonFile.writeAsStringSync(jsonEncode(spotPrices));

    print(jsonFile.absolute);

    // Ask the model to predict
    ProcessResult result = Process.runSync('python', [modelScript, jsonFile.absolute.toString()]);
    if( result.stderr != null ){
      console.setForegroundColor(ConsoleColor.red);
      console.writeLine(result.stderr);
      return;
    }

    File responseFile = new File("expectTime.json");  // Read the out file data from the prediction script
    if( !responseFile.existsSync() ){
      console.setForegroundColor(ConsoleColor.red);
      console.writeLine("Unable to find the predicted response file: " + result.stdout);
      return;
    }

    List modelResponseList = jsonDecode(responseFile.readAsStringSync());

    await loadFuture; // ensure that the pricing CSV is loaded

    // Determine the specs and OS which will be used for hosting the app
    Map<String, dynamic> instanceMap = findOptimalInstance(modelResponseList, getCurrentSpotPrices(spotPrices));
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
      uploadToS3(model, id, "data.npz"),
      uploadToS3(script, id, "script.py"),
    ]);

    final interruptTime = DateTime.now().add(Duration(minutes: int.parse(instance['time'].toString()))).toUtc().toIso8601String();

    // save entry to database
    Process.runSync('aws', ['dynamodb', 'put-item', '--table-name', 'jobs', '--item', 
      '{"id": {"S":"$id"}, "name": {"S":"$jobName"}, "instance": {"S":"${instance['instanceType']}"},' 
      '"location":{"S":"${instance['region']}"}, "repl":{"N":"1"}, '
      '"startTime":{"S":"${DateTime.now().toUtc().toIso8601String()}"}, "status":{"S":"INITIALIZING"},'
      '"estimatedInterrupt":{"S":"$interruptTime"}, "restarts": {"S":"0"}, "operatingSystem":{"S":"${instance['os']}"},'
      '"bidPrice":{"S":"${instance['price']}"}, "interruptMinutes":{"S":"${instance['time']}"},'
      '"spotPrice":{"S":"${instanceMap['SpotPrice']}"}, "checkpoint": {"S":""}, "sir":{"S":""}}'
    ]); // checkpoint here is the time of the last checkpoint. sir is the Spot instance request ID

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

    console.setForegroundColor(ConsoleColor.red);
    console.writeLine(response["error"]);

    // delete the uploaded files from S3
    Process.runSync('aws', ['s3', 'rm', "s3://comp598-deepspot/$id", '--recursive']);
    
    // remove the entry from the database
    Process.runSync('aws', ['dynamodb', 'put-item', '--table-name', 'jobs', '--key', 
      '{"id": {"S":"$id"}}'
    ]);
  }
} 

Future uploadToS3(String localFilePath, String appId, String s3Name){
  return Process.run('aws', ['s3', 'cp', localFilePath, "s3://comp598-deepspot/$appId/$s3Name"]);
}

Future downloadModelFromS3(String appId){
  return Process.run('aws', ['s3', 'cp', "s3://comp598-deepspot/$appId/model.h5", "$appId-model.h5"]);
}

Future<List> getSpotPrices(DateTime startTime, DateTime endTime) async{
  List spotPrices = [];
  Map dataMap;
  for(String region in regions){
    while(true){
      ProcessResult result = Process.runSync('aws', ['ec2', 'describe-spot-price-history', 
        '--start-time', startTime.toIso8601String().split(".")[0],
        '--end-time', endTime.toIso8601String().split(".")[0],
        '--filters', 'Name=instance-type,Values=p*',
        '--region', region,
        '--max-items', '1000',
        if( dataMap != null && dataMap.containsKey("NextToken") )
          ...['--starting-token', dataMap['NextToken']]
      ]);
      dataMap = jsonDecode(result.stdout);
      spotPrices.addAll(dataMap['SpotPriceHistory']);
      
      if( !dataMap.containsKey("NextToken") )
        break;
    }
  }

  return spotPrices;
}

/// Get the most recent spot instance price for all instances in all regions
Map getCurrentSpotPrices(List allSpotPrices){
  Map<String, dynamic> map = {};

  allSpotPrices
    .where((item) => item["ProductDescription"] == "Linux/UNIX")
    .forEach((item){
      String key = "${item['AvailabilityZone']}-${item['InstanceType']}-${item['ProductDescription']}";
      if( !map.containsKey(key) || DateTime.parse(item["Timestamp"]).isAfter(DateTime.parse(map[key]["Timestamp"])) )
        map[key] = item;
    });

  return map;
}

/// Load the ondemand pricing from the CSV file
Future loadOnDemandPricing() async{
  if( pricing == null ){
    final file = new File("pricing.csv").openRead();
    final list = await file.transform(utf8.decoder).transform(CsvToListConverter())
      .where((elem) => elem[16] == "Location" || regionNamesMap.containsKey(elem[16]))  // Remove all entries outside supported regions
      .where((elem) => elem[37] == "Operating System" || elem[37] == "Linux") // Reomve all entries that are not Linux OS
      .toList();

    final keys = list[0];
    final columns = [0, 4, 9, 16, 18, 21, 24, 26, 53];  // Useful columns in the CSV file
    pricing = {};

    list.skip(1).forEach((elem){
      Map<String, dynamic> map = {};
      for(var column in columns)
        map[keys[column]] = elem[column];

      map['InstanceType'] = map['Instance Type'];
      map['region'] = regionNamesMap[map["Location"]];
      map['ProductDescription'] = "Linux/UNIX";

      String key1 = "${map['region']}a-${map['InstanceType']}-Linux/UNIX";  
      String key2 = "${map['region']}b-${map['InstanceType']}-Linux/UNIX";
      String key3 = "${map['region']}c-${map['InstanceType']}-Linux/UNIX";

      pricing[key1] = map;
      pricing[key2] = map;
      pricing[key3] = map;
    });
  }

  return true;
}

/// Required keys for params are: os, region, instanceType, availabilityZone, id, price
Map<String, dynamic> requestSpotInstance(Map<String, dynamic> params){
  // Create launch specs using the ID passed in params.
  Map<String, dynamic> specs = {
    "ImageId": ami[params["os"]][params["region"]],
    "KeyName": "Default-Key-Pair",
    "SecurityGroupIds": ["sg-0caf14f9eccb80727"],
    "InstanceType": params["instanceType"],
    "Placement": {
      "AvailabilityZone": params['availabilityZone']
    },
    "NetworkInterfaces": [
      {
        "DeviceIndex": 0,
        "AssociatePublicIpAddress": true
      }
    ],
    "IamInstanceProfile": {
      "Arn": "arn:aws:iam::413168166423:user/comp598"
    },
    "UserData": base64Encode(utf8.encode(new File("unix-launch.sh").readAsStringSync().replaceAll("{JOBID}", params['id'])))
  };

  // Write specs to file
  File jsonFile = new File("${params['id']}.json");
  jsonFile.writeAsStringSync(jsonEncode(specs));

  ProcessResult result = Process.runSync('aws', ['ec2', 'request-spot-instances',
    '--spot-price', params['price'],
    '--client-token', params['id'],
    '--availability-zone-group', params['availabilityZone'],
    '--launch-specification', "file://${params['id']}.json",
  ]);

  if( result.stderr != null && result.stderr.toString().isNotEmpty )
    return {"status": false, "error": result.stderr.toString()};
  
  return {"status": true, "payload": jsonDecode(result.stdout)};
}

Map<String, dynamic> findOptimalInstance(List modelResponse, Map currentSpotPrices){
  // Filter and sort response
  modelResponse
    // Filter for only the Linux OS
    ..where((elem) => elem["ProductDescription"] == "Linux/UNIX")
    // Sort the List and bring the p2 instances first
    ..sort((map1, map2){
      return map1['InstanceType'].toString().compareTo(map2['InstanceType'].toString());
    });

  // For now, find the first entry greater than 30 mins. Later we can optimize this to get a better time
  return modelResponse.firstWhere((item){
    // For now skip all those that the prices do not change???
    if( item["FLAG"] == 0 )
      return false;

    String key = "${item['AvailabilityZone']}-${item['InstanceType']}-${item['ProductDescription']}";
    // Skip if we do not have any current spot price for this predicted data
    if( !currentSpotPrices.containsKey(key) )
      return false;

    // Skip if we do not have an ondemand price for this predicted data
    if( !pricing.containsKey(key) )
      return false;

    var time = 0;
    Map r = item['R'];
    var spotPrice = currentSpotPrices[key]["SpotPrice"];
    var onDemandPrice = pricing[key]["PricePerUnit"];

    for( String key in r.keys ){
      if( r[key] == null || r[key] == -1 || time > r[key] 
        || int.parse(key.split("_")[1]) * spotPrice > onDemandPrice ) // If the increase in the current spot price will exceed the on demand price
        break;

      time = r[key];
    }

    // Check if we can squeeze 30 minutes or more before the spot instance price reaches the ondemand price
    if( time > 30 ){
      item['time'] = time;
      item['price'] = onDemandPrice;
      return true;
    }

    return false;
  });
}

printError(String message){
  console.setForegroundColor(ConsoleColor.red);
  console.writeErrorLine(message);
}