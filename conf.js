var conf = {};
var cse = {};
var ae = {};
var ngsild = {};

//cse config
cse.host = "203.253.128.161";
cse.port = "7579";
cse.name = "Mobius";
cse.id = "/Mobius2";
cse.mqttport = "1883";

//ae config
ae.name = "office2017";
ae.id = "S" + ae.name;
ae.parent = "/" + cse.name;
ae.appid = "office2017"

//lora config
ngsild.host = "203.253.128.164";
ngsild.mqttport = "1883";

conf.cse = cse;
conf.ae = ae;
conf.ngsild = ngsild;

module.exports = conf;
