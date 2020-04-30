import 'dart:convert';
import 'dart:io';

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

  if( result.stderr != null ){
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