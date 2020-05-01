import 'dart:convert';
import 'dart:io';

import 'package:csv/csv.dart';

Map<String, dynamic> pricing;

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


bool checkJobExists(String jobId){
  return getJobDetails(jobId) != null;
}

Map getJobDetails(String jobId){
  ProcessResult results = Process.runSync('aws', ['dynamodb', 'scan', '--table-name', 'jobs', 
      '--filter-expression', '#ST = :a', 
      '--expression-attribute-names', '{"#ST": "id"}',
      '--expression-attribute-values', '{":a":{"S":"$jobId"}}']);
  
  Map map = jsonDecode(results.stdout);
  
  return map["Items"].isEmpty ? null : map["Items"][0];
}

updateJobEntry(String jobId, String key, String value){
  Process.runSync('aws', ['dynamodb', 'update-item', '--table-name', 'jobs', '--key', 
    '{"id": {"S":"$jobId"}}', '--update-expression', 'SET #ST = :val',
    '--expression-attribute-names', '{"#ST": "$key"}',
    '--expression-attribute-values', '{":val":{"S":"$value"}}'
  ]);
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

/// sir is the Spot instance request ID
cancelSpotInstanceRequest(String sir){
  Process.runSync('aws', ['ec2', 'cancel-spot-instance-requests', '--spot-instance-request-ids', sir]);
}

/// Given the Spot Instance request ID, get the Instance Id
String sirToSpotInstanceId(String sir){
  final result = Process.runSync('aws', ['ec2', 'describe-spot-instance-requests', 
    '--filters', 'Name=spot-instance-request-id,Values=$sir',
    '--query', 'SpotInstanceRequests[*].{ID:InstanceId}'
  ]);

  if( result.stderr != null && result.stderr.toString().isNotEmpty ){
    print(result.stderr);
    return null;
  }
 
  List list = jsonDecode(result.stdout);
  if( list.isEmpty )
    return null;

  return list[0]["ID"];
}

terminateInstance(String instanceId){
  Process.runSync('aws', ['ec2', 'terminate-instances', '--instance-ids', instanceId]);
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
    "SecurityGroupIds": ["sg-0caf14f9eccb80727"], //sg-0c3c674870aedb77b
    "InstanceType": params["instanceType"],
    "Placement": {
      "AvailabilityZone": params['availabilityZone']
    },
    // "NetworkInterfaces": [
    //   {
    //     "DeviceIndex": 0,
    //     "AssociatePublicIpAddress": true
    //   }
    // ],
    "IamInstanceProfile": {
      "Arn": "arn:aws:iam::413168166423:user/deepspot"
    },
    "UserData": base64Encode(utf8.encode(new File("unix-launch.sh").readAsStringSync().replaceAll("{JOBID}", params['id'])))
  };

  // Write specs to file
  File jsonFile = new File("${params['id']}.json");
  jsonFile.writeAsStringSync(jsonEncode(specs));

  ProcessResult result = Process.runSync('aws', ['ec2', 'request-spot-instances',
    '--spot-price', params['price'].toString(),
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

    var time = 0.0;
    Map r = item['R'];
    var spotPrice = double.parse(currentSpotPrices[key]["SpotPrice"].toString());
    var onDemandPrice = double.parse(pricing[key]["PricePerUnit"].toString());
    
    for( String key in r.keys ){
      var t = double.parse(r[key].toString());
      if( r[key] == null || t <= 0 || time > t 
        || double.parse(key.split("_")[1]) * spotPrice > onDemandPrice ) // If the increase in the current spot price will exceed the on demand price
        break;

      time = t;
    }

    // Check if we can squeeze 30 minutes or more before the spot instance price reaches the ondemand price
    if( time * 60 > 30 ){ // Time is in hours so convert to minutes
      item['time'] = (time * 60).toInt(); // Convert to minutes
      item['price'] = onDemandPrice;
      return true;
    }
    else
      print(time);

    return false;
  }, orElse: () => null);
}