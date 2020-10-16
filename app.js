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
    var timeformat = year+"-"+month+"-"+day+"T"+hour+":"+minute+":"+sec;
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
            body["illuminance"].value=parseFloat(cinObj.con);
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
            body["humidity"].value=parseFloat(cinObj.con);
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
            body["PIR"].value=parseFloat(cinObj.con);
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
            if(cinObj.con.length >=4){
                body["temperature"].value=parseFloat(cinObj.con);
            }
            else{
                body["temperature"].value=parseFloat(cinObj.con)+0.1;
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
            body["co2"].value=parseFloat(cinObj.con);
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
        var jsonObj = JSON.parse(message.toString());
        if (jsonObj['m2m:rqp'] == null) {
            jsonObj['m2m:rqp'] = jsonObj;
        }

        mqtt_noti_action(jsonObj, function (path_arr, cinObj, rqi, sur) {
            if (cinObj) {
                var rsp_topic = '/oneM2M/resp/' + topic_arr[3] + '/' + topic_arr[4] + '/' + topic_arr[5];

                event.emit('upload', sur, cinObj);

                response_mqtt(rsp_topic, '2001', '', conf.ae.id, rqi, '', topic_arr[5]);
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

    mqtt_client.publish(rsp_topic, JSON.stringify(rsp_message['m2m:rsp']));

    console.log('noti publish -> ' + JSON.stringify(rsp_message));

}

function init_resource(){
    read_sensor_id_list();
    var sub_ae_parent_path = conf.ae.parent + '/' + conf.ae.name;
    for (var i = 0; i < place_ids.length; i++) {
        var sub_ipe = sub_ae_parent_path + '/'+ place_ids[i].toLowerCase();
        var sub_body = {nu:['mqtt://' + conf.cse.host  +'/'+ conf.ae.id + '?ct=json']};
        var sub_obj = {
            'm2m:sub':
                {
                    'rn' : "sub_ipe",
                    'enc': {'net': [1,2,3,4,5]},
                    'nu' : sub_body.nu,
                    'nct': 2
                }
        };
        var sub_path = sub_ipe +'/'+"sub_ipe";
        var resp_sub = keti_mobius.retrieve_sub(sub_path);

        if (resp_sub.code == 200) {
            resp_sub = keti_mobius.delete_res(sub_path);

            if (resp_sub.code == 200) {
                resp_sub = keti_mobius.create_sub(sub_ipe, sub_obj);

            }
        }
        else if (resp_sub.code == 404) {
            keti_mobius.create_sub(sub_ipe, sub_obj);
        }
        else{

        }
        if(resp_sub.code == 201 || resp_sub.code == 409){
           console.log("SUB_Complete!!");
        }
    }
    init_mqtt_client();
}
 function parse_sgn(rqi, pc, callback) {
    if(pc.sgn) {
        var nmtype = pc['sgn'] != null ? 'short' : 'long';
        var sgnObj = {};
        var cinObj = {};
        sgnObj = pc['sgn'] != null ? pc['sgn'] : pc['singleNotification'];

        if (nmtype === 'long') {
            console.log('oneM2M spec. define only short name for resource')
        }
        else { // 'short'
            if (sgnObj.sur) {
                if(sgnObj.sur.charAt(0) != '/') {
                    sgnObj.sur = '/' + sgnObj.sur;
                }
                var path_arr = sgnObj.sur.split('/');
            }

            if (sgnObj.nev) {
                if (sgnObj.nev.rep) {
                    if (sgnObj.nev.rep['m2m:cin']) {
                        sgnObj.nev.rep.cin = sgnObj.nev.rep['m2m:cin'];
                        delete sgnObj.nev.rep['m2m:cin'];
                    }

                    if (sgnObj.nev.rep.cin) {
                        cinObj = sgnObj.nev.rep.cin;
                    }
                    else {
                        console.log('[mqtt_noti_action] m2m:cin is none');
                        cinObj = null;
                    }
                }
                else {
                    console.log('[mqtt_noti_action] rep tag of m2m:sgn.nev is none. m2m:notification format mismatch with oneM2M spec.');
                    cinObj = null;
                }
            }
            else if (sgnObj.sud) {
                console.log('[mqtt_noti_action] received notification of verification');
                cinObj = {};
                cinObj.sud = sgnObj.sud;
            }
            else if (sgnObj.vrq) {
                console.log('[mqtt_noti_action] received notification of verification');
                cinObj = {};
                cinObj.vrq = sgnObj.vrq;
            }

            else {
                console.log('[mqtt_noti_action] nev tag of m2m:sgn is none. m2m:notification format mismatch with oneM2M spec.');
                cinObj = null;
            }
        }
    }
    else {
        console.log('[mqtt_noti_action] m2m:sgn tag is none. m2m:notification format mismatch with oneM2M spec.');
        console.log(pc);
    }

    callback(path_arr, cinObj, rqi);
};

function mqtt_noti_action(jsonObj, callback) {
    if (jsonObj != null) {
        var op = (jsonObj['m2m:rqp']['op'] == null) ? '' : jsonObj['m2m:rqp']['op'];
        var to = (jsonObj['m2m:rqp']['to'] == null) ? '' : jsonObj['m2m:rqp']['to'];
        var fr = (jsonObj['m2m:rqp']['fr'] == null) ? '' : jsonObj['m2m:rqp']['fr'];
        var rqi = (jsonObj['m2m:rqp']['rqi'] == null) ? '' : jsonObj['m2m:rqp']['rqi'];
        var pc = {};
        pc = (jsonObj['m2m:rqp']['pc'] == null) ? {} : jsonObj['m2m:rqp']['pc'];
        if(pc['m2m:sgn']) {
            pc.sgn = {};
            pc.sgn = pc['m2m:sgn'];
            delete pc['m2m:sgn'];
        }
        parse_sgn(rqi, pc, function(path_arr, cinObj,rqi){
            if(cinObj) {
                if(cinObj.sud || cinObj.vrq) {
                    var resp_topic = '/oneM2M/resp/' + topic_arr[3] + '/' + topic_arr[4] + '/' + topic_arr[5];
                    // _this.response_mqtt(resp_topic, 2001, '', conf.ae.id, rqi, '', topic_arr[5]);
                }
                else {
                                console.log('mqtt ' + 'json' + ' notification <----');
                                var sur = pc.sgn.sur.split('/');
                                if(pc.sgn.nev.net == '3'){

                                    var cnt_id = sur[3].toLowerCase();
                                    var cr_time = pc.sgn.nev.rep.cin.ct;
                                    ngsild_post(cnt_id,cinObj,cr_time);
                                }
                                callback(path_arr, cinObj, rqi, pc.sgn.sur);
                }
            }
        })
    }
    else {
        console.log('[mqtt_noti_action] message is not noti');
    }
}

setTimeout(init_resource,100);