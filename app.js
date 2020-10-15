var events = require('events');
var mqtt = require('mqtt');
var util = require('util');
var fs = require('fs');
var jsonpath = require('jsonpath');
var place_ids = [];
var mobius = require('./MobiusConnector').mobius;
const SENSOR_PLACE_FILE = 'sensor_place.txt';
global.conf = require('./conf.js');
var event = new events.EventEmitter();
var keti_mobius = new mobius();
keti_mobius.set_mobius_info(conf.cse.host, conf.cse.port);
var http = require('http');
var ngsild_host = '172.20.0.129';


function time_convert(cr_time){
    cr_time = cr_time.toString().split('T');
    var year = cr_time[0].substring(0,4);
    var month = cr_time[0].substring(4,6);
    var day = cr_time[0].substring(6,8);
    var hour= cr_time[1].substring(0,2);
    var minute = cr_time[1].substring(2,4);
    var sec = cr_time[1].substring(4,6);
    var timeformat = year+"-"+month+"-"+day+"T"+hour+":"+minute+":"+sec+"Z";
    return timeformat
}

function ngsild_post(cnt_id,cinObj,cr_time){
    let body = {}
    var arr = [];
    cnt_id = cnt_id.split('_');
    if(cnt_id.length >=2){
        if(cnt_id[0] == 'light'){
            //Ill
            var light_id = 'Ill'+cnt_id[2];
            var ct = time_convert(cr_time);
            body["id"] = "urn:ngsi-ld:Sensor:Sensor" +cnt_id[2];
            body["modifiedAt"] = ct;
            body["illuminance"]={};
            body["illuminance"].type = "Property";
            body["illuminance"].value=parseFloat(cinObj[0]);
            arr[0] = body;
        }
        else if (cnt_id[0] == 'hum'){
            //Hum
            var hum_id = 'Hum'+cnt_id[2];
            var ct = time_convert(cr_time);
            // console.log(hum_id+'/'+cinObj+'/'+ct);
            body["id"] = "urn:ngsi-ld:Sensor:Sensor" +cnt_id[2];
            body["modifiedAt"] = ct;
            body["humidity"]={};
            body["humidity"].type = "Property";
            body["humidity"].value=parseFloat(cinObj[0]);
            arr[0] = body;
        }
        else if(cnt_id[0] == 'pir'){
            //PIR
            var pir_id = 'PIR'+cnt_id[2];
            var ct = time_convert(cr_time);
            // console.log(pir_id+'/'+cinObj+'/'+ct);
            body["id"] = "urn:ngsi-ld:Sensor:Sensor" +cnt_id[2];
            body["modifiedAt"] = ct;
            body["PIR"]={};
            body["PIR"].type = "Property";
            body["PIR"].value=parseFloat(cinObj[0]);
            arr[0] = body;
        }
        else if(cnt_id[0] == 'temp'){
            //Temp
            var temp_id = 'Temp'+cnt_id[2];
            var ct = time_convert(cr_time);
            // console.log(temp_id+'/'+cinObj+'/'+ct);
            body["id"] = "urn:ngsi-ld:Sensor:Sensor" +cnt_id[2];
            body["modifiedAt"] = ct;
            body["temperature"]={};
            body["temperature"].type = "Property";
            if(cinObj[0].length >=4){
                body["temperature"].value=parseFloat(cinObj[0]);
            }
            else{
                body["temperature"].value=parseFloat(cinObj[0])+0.1;
            }
            arr[0] = body;
        }
        else if(cnt_id[0] == 'co2'){
            //Co
            var co_id = 'Co'+cnt_id[2];
            var ct = time_convert(cr_time);
            // console.log(co_id+'/'+cinObj+'/'+ct);
            body["id"] = "urn:ngsi-ld:Sensor:Sensor" +cnt_id[2];
            body["modifiedAt"] = ct;
            body["co2"]={};
            body["co2"].type = "Property";
            body["co2"].value=parseFloat(cinObj[0]);
            arr[0] = body;
        }
        else{
            console.log("Dose not exist");
        }
        var payload_message = JSON.stringify(arr);
        console.log(payload_message);
        var options = {
            'method': 'POST',
            'host': ngsild_host,
            'port': 8080,
            'path': '/entityOperations/update',

            'headers': {
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(payload_message)
            }
        };
        var post_req = http.request(options, function(res) {
            res.setEncoding('utf8');
            res.on('data', function (chunk) {
                console.log('Response: ' + chunk);
            });
        });
        post_req.write(payload_message);
        post_req.end();
    }
}

function read_sensor_id_list(){
    var str = String(fs.readFileSync(SENSOR_PLACE_FILE));
    place_ids = str.split(',');
    console.log(place_ids);
    for (i = 0; i < place_ids.length; i++){
        console.log(place_ids[i]);
        place_ids[i] = JSON.parse(place_ids[i]);
        place_ids[i] = place_ids[i]["id"];
    }
    console.log(place_ids);
}

function init_mqtt_client() {
    var mobius_connectOptions = {
        host: conf.cse.host,
        port: conf.cse.mqttport,
        protocol: "mqtt",
        keepalive: 10,
        protocolId: "MQTT",
        protocolVersion: 4,
        clean: true,
        reconnectPeriod: 2000,
        connectTimeout: 2000,
        rejectUnauthorized: false
    };
    mqtt_client = mqtt.connect(mobius_connectOptions);
    mqtt_client.on('connect', on_mqtt_connect);
    mqtt_client.on('message', on_mqtt_message_recv);
    console.log("init_mqtt_client!!!");
}



function on_mqtt_connect() {
        var noti_topic = util.format('/oneM2M/req/+/%s/#', conf.ae.id);
        mqtt_client.unsubscribe(noti_topic);
        mqtt_client.subscribe(noti_topic);
        console.log('[mqtt_connect] noti_topic : ' + noti_topic);
}

function on_mqtt_message_recv(topic, message) {
    console.log('receive message from topic: <- ' + topic);
    console.log('receive message: ' + message.toString());
    var topic_arr = topic.split("/");
    if (topic_arr[1] == 'oneM2M' && topic_arr[2] == 'req' && topic_arr[4] == conf.ae.id) {
        var jsonObj = JSON.parse(JSON.stringify(message.toString()));
        mqtt_noti_action(jsonObj, function (path_arr, cinObj, rqi, sur) {
            if (cinObj) {
                var rsp_topic = '/oneM2M/resp/' + topic_arr[3] + '/' + topic_arr[4] + '/' + topic_arr[5];

                event.emit('upload', sur, cinObj);

                response_mqtt(rsp_topic, '2000', '', conf.ae.id, rqi, '');
            }
        });
    }
    else {
        console.log('topic is not supported');
    }
}

function response_mqtt (rsp_topic, rsc, to, fr, rqi, inpcs) {

    var rsp_message = {};
    rsp_message['m2m:rsp'] = {};
    rsp_message['m2m:rsp'].rsc = rsc;
    rsp_message['m2m:rsp'].to = to;
    rsp_message['m2m:rsp'].fr = fr;
    rsp_message['m2m:rsp'].rqi = rqi;
    rsp_message['m2m:rsp'].pc = inpcs;

    mqtt_client.publish(rsp_topic, JSON.stringify(rsp_message));

    console.log('noti publish -> ' + JSON.stringify(rsp_message));

}

function init_resource(){
    read_sensor_id_list();
    var sub_ae_parent_path = conf.ae.parent + '/' + conf.ae.name;
    for (var i = 0; i < place_ids.length; i++) {
        var sub_downlink = sub_ae_parent_path + '/'+ place_ids[i].toLowerCase();
        var sub_body = {nu:['mqtt://' + conf.cse.host  +'/'+ conf.ae.id + '?ct=json']};
        var sub_obj = {
            'm2m:sub':
                {
                    'rn' : "sub_ipe",
                    'enc': {'net': [3]},
                    'nu' : sub_body.nu,
                    'nct': 2
                }
        };
        var sub_path = sub_downlink +'/'+"sub_ipe";
        var resp_sub = keti_mobius.retrieve_sub(sub_path);

        if (resp_sub.code == 200) {
            resp_sub = keti_mobius.delete_res(sub_path);

            if (resp_sub.code == 200) {
                resp_sub = keti_mobius.create_sub(sub_downlink, sub_obj);

            }
        }
        else if (resp_sub.code == 404) {
            keti_mobius.create_sub(sub_downlink, sub_obj);
        }
        else{

        }
        if(resp_sub.code == 201 || resp_sub.code == 409){
           console.log("SUB_Complete!!");
        }
    }
    init_mqtt_client();
}

function mqtt_noti_action(jsonObj, callback) {
    if (jsonObj != null) {
        var net =  JSON.stringify(jsonpath.query(JSON.parse(jsonObj), '$..net'));
        net=net.replace("\"", "").replace("]", "").replace("[", "").replace("\"", "");
        var path_arr= JSON.stringify(jsonpath.query(JSON.parse(jsonObj),'$..sur'));
        var cinObj= jsonpath.query(JSON.parse(jsonObj),'$..con');
        var cr_time= jsonpath.query(JSON.parse(jsonObj),'$..ct');

        var sur = path_arr.split('/');

        // console.log("#####"+cr_time);
        if(net == '3'){
            var cnt_id = sur[2].toLowerCase();
            ngsild_post(cnt_id,cinObj,cr_time);
        }
        else if (net == '4'){
            var cnt_parent_path = conf.ae.parent + '/' + conf.ae.name;
            var rn =  JSON.stringify(jsonpath.query(JSON.parse(jsonObj), '$..rn'));
            rn=rn.replace("\"", "").replace("]", "").replace("[", "").replace("\"", "");
            console.log(rn);
            var retry_cnt_sensor_obj = {
                'm2m:cnt':{
                'rn' : rn
                }
            };
            var cnt_resp = keti_mobius.create_cnt(cnt_parent_path, retry_cnt_sensor_obj);
            if (cnt_resp.code == 201 || cnt_resp.code == 409){
                var cnt2_parent_path = cnt_parent_path +'/'+ rn;
                var cnt_upobj = {
                    'm2m:cnt':{
                    'rn' : "up"
                    }
                };
                var cnt_downobj = {
                    'm2m:cnt':{
                    'rn' : "down"
                    }
                };
                var cnt_resp2 = keti_mobius.create_cnt(cnt2_parent_path, cnt_upobj);
                if(cnt_resp2.code == 201 || cnt_resp2.code == 409){
                   var cnt_resp3 = keti_mobius.create_cnt(cnt2_parent_path, cnt_downobj);
                }
                if(cnt_resp3.code == 201 || cnt_resp3.code == 409 ){
                    console.log("Container Recreate!");
                    var sub_parent_path = cnt2_parent_path +'/'+"down";
                    var sub_body = {nu:['mqtt://' + conf.cse.host  +'/'+ conf.ae.id + '?ct=json']};
                    var sub_obj = {
                        'm2m:sub':
                            {
                                'rn' : "sub_lora_sensor",
                                'enc': {'net': [3]},
                                'nu' : sub_body.nu,
                                'nct': 2
                            }
                    };
                    var sub_path = sub_parent_path +'/'+"sub_lora_sensor";
                    var resp_sub = keti_mobius.retrieve_sub(sub_path);

                    if (resp_sub.code == 200) {
                        resp_sub = keti_mobius.create_sub(sub_parent_path, sub_obj);

                    }
                    else if (resp_sub.code == 404) {
                        keti_mobius.create_sub(sub_parent_path, sub_obj);
                    }
                }
            }
        }
    }
    else {
        console.log('[mqtt_noti_action] message is not noti');
    }
}

setTimeout(init_resource,100)
