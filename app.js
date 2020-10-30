var events = require('events');
var mqtt = require('mqtt');
var util = require('util');
var fs = require('fs');
var place_ids = [];
var mobius = require('./MobiusConnector').mobius;
const SENSOR_PLACE_FILE = 'sensor_place.txt';
var dateFormat = require('dateformat');
global.conf = require('./conf.js');
var config_list = require('./placelist.json');

var event = new events.EventEmitter();
var keti_mobius = new mobius();
keti_mobius.set_mobius_info(conf.cse.host, conf.cse.port);
var http = require('http');
var ngsild_host = '172.20.0.129';

var list_keys = Object.keys(config_list);
var obj = {};
obj["filab_01"] = config_list["filab_01"];
obj["filab_02"] = config_list["filab_02"];
obj["filab_05"] = config_list["filab_05"];
obj["filab_06"] = config_list["filab_06"];
obj["filab_07"] = config_list["filab_07"];
obj["filab_08"] = config_list["filab_08"];
obj["filab_09"] = config_list["filab_09"];
obj["filab_11"] = config_list["filab_11"];
obj["filab_12"] = config_list["filab_12"];
obj["filab_13"] = config_list["filab_13"];
obj["filab_14"] = config_list["filab_14"];
obj["filab_15"] = config_list["filab_15"];
obj["filab_16"] = config_list["filab_16"];

function timestemp(){

    var date = new Date(Date.now());
    date = date.toISOString();
    date = dateFormat(date,"isoUtcDateTime");
    console.log(date)
    return date
}

function ngsild_post(cnt_id,cinObj,cr_time){
    var ct = timestemp();

    cnt_id = cnt_id.split('_');
    console.log(cnt_id);
    if(cnt_id[2] == "01"){
        obj.filab_01["modifiedAt"] = ct;
        if(cnt_id[0] == "temp"){
            if(cinObj.con[3] != '0'){
                obj.filab_01["temperature"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_01["temperature"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "hum"){
            obj.filab_01["humidity"].value=parseFloat(cinObj.con);
        }
        else if(cnt_id[0] == "light"){
            if(cinObj.con[4] != '0'){
                obj.filab_01["illuminance"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_01["illuminance"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "pir"){
            obj.filab_01["pir"].value=parseFloat(cinObj.con);
        }
    }
    if(cnt_id[2] == "02"){
        obj.filab_02["modifiedAt"] = ct;
        if(cnt_id[0] == "temp"){
            if(cinObj.con[3] != '0'){
                obj.filab_02["temperature"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_02["temperature"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "hum"){
            obj.filab_02["humidity"].value=parseFloat(cinObj.con);
        }
        else if(cnt_id[0] == "light"){
            if(cinObj.con[4] != '0'){
                obj.filab_02["illuminance"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_02["illuminance"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "pir"){
            obj.filab_02["pir"].value=parseFloat(cinObj.con);
        }
        else if(cnt_id[0] == "co2"){
            obj.filab_02["co2"].value=parseFloat(cinObj.con);
        }
    }
    if(cnt_id[2] == "05"){
        obj.filab_05["modifiedAt"] = ct;
        if(cnt_id[0] == "temp"){
            if(cinObj.con[3] != '0'){
                obj.filab_05["temperature"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_05["temperature"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "hum"){
            obj.filab_05["humidity"].value=parseFloat(cinObj.con);
        }
        else if(cnt_id[0] == "light"){
            if(cinObj.con[4] != '0'){
                obj.filab_05["illuminance"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_05["illuminance"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "pir"){
            obj.filab_05["pir"].value=parseFloat(cinObj.con);
        }
    }
    if(cnt_id[2] == "06"){
        obj.filab_06["modifiedAt"] = ct;
        if(cnt_id[0] == "temp"){
            if(cinObj.con[3] != '0'){
                obj.filab_06["temperature"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_06["temperature"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "hum"){
            obj.filab_06["humidity"].value=parseFloat(cinObj.con);
        }
        else if(cnt_id[0] == "light"){
            if(cinObj.con[4] != '0'){
                obj.filab_06["illuminance"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_06["illuminance"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "pir"){
            obj.filab_06["pir"].value=parseFloat(cinObj.con);
        }
    }
    if(cnt_id[2] == "07"){
        obj.filab_07["modifiedAt"] = ct;
        if(cnt_id[0] == "temp"){
            if(cinObj.con[3] != '0'){
                obj.filab_07["temperature"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_07["temperature"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "hum"){
            obj.filab_07["humidity"].value=parseFloat(cinObj.con);
        }
        else if(cnt_id[0] == "light"){
            if(cinObj.con[4] != '0'){
                obj.filab_07["illuminance"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_07["illuminance"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "pir"){
            obj.filab_07["pir"].value=parseFloat(cinObj.con);
        }
    }
    if(cnt_id[2] == "08"){
        obj.filab_08["modifiedAt"] = ct;
        if(cnt_id[0] == "temp"){
            if(cinObj.con[3] != '0'){
                obj.filab_08["temperature"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_08["temperature"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "hum"){
            obj.filab_08["humidity"].value=parseFloat(cinObj.con);
        }
        else if(cnt_id[0] == "light"){
            if(cinObj.con[4] != '0'){
                obj.filab_08["illuminance"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_08["illuminance"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "pir"){
            obj.filab_08["pir"].value=parseFloat(cinObj.con);
        }
    }
    if(cnt_id[2] == "09"){
        obj.filab_09["modifiedAt"] = ct;
        if(cnt_id[0] == "temp"){
            if(cinObj.con[3] != '0'){
                obj.filab_09["temperature"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_09["temperature"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "hum"){
            obj.filab_09["humidity"].value=parseFloat(cinObj.con);
        }
        else if(cnt_id[0] == "light"){
            if(cinObj.con[4] != '0'){
                obj.filab_09["illuminance"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_09["illuminance"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "pir"){
            obj.filab_09["pir"].value=parseFloat(cinObj.con);
        }
        else if(cnt_id[0] == "co2"){
            obj.filab_09["co2"].value=parseFloat(cinObj.con);
        }
    }
    if(cnt_id[2] == "11"){
        obj.filab_11["modifiedAt"] = ct;
        if(cnt_id[0] == "temp"){
            if(cinObj.con[3] != '0'){
                obj.filab_11["temperature"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_11["temperature"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "hum"){
            obj.filab_11["humidity"].value=parseFloat(cinObj.con);
        }
        else if(cnt_id[0] == "light"){
            if(cinObj.con[4] != '0'){
                obj.filab_11["illuminance"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_11["illuminance"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "pir"){
            obj.filab_11["pir"].value=parseFloat(cinObj.con);
        }
    }
    if(cnt_id[2] == "12"){
        obj.filab_12["modifiedAt"] = ct;
        if(cnt_id[0] == "temp"){
            if(cinObj.con[3] != '0'){
                obj.filab_12["temperature"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_12["temperature"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "hum"){
            obj.filab_12["humidity"].value=parseFloat(cinObj.con);
        }
        else if(cnt_id[0] == "light"){
            if(cinObj.con[4] != '0'){
                obj.filab_12["illuminance"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_12["illuminance"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "pir"){
            obj.filab_12["pir"].value=parseFloat(cinObj.con);
        }
        else if(cnt_id[0] == "co2"){
            obj.filab_12["co2"].value=parseFloat(cinObj.con);
        }
    }
    if(cnt_id[2] == "13"){
        obj.filab_13["modifiedAt"] = ct;
        if(cnt_id[0] == "temp"){
            if(cinObj.con[3] != '0'){
                obj.filab_13["temperature"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_13["temperature"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "hum"){
            obj.filab_13["humidity"].value=parseFloat(cinObj.con);
        }
        else if(cnt_id[0] == "light"){
            if(cinObj.con[4] != '0'){
                obj.filab_13["illuminance"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_13["illuminance"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "pir"){
            obj.filab_13["pir"].value=parseFloat(cinObj.con);
        }
    }
    if(cnt_id[2] == "14"){
        obj.filab_14["modifiedAt"] = ct;
        if(cnt_id[0] == "temp"){
            if(cinObj.con[3] != '0'){
                obj.filab_14["temperature"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_14["temperature"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "hum"){
            obj.filab_14["humidity"].value=parseFloat(cinObj.con);
        }
        else if(cnt_id[0] == "light"){
            if(cinObj.con[4] != '0'){
                obj.filab_14["illuminance"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_14["illuminance"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "pir"){
            obj.filab_14["pir"].value=parseFloat(cinObj.con);
        }
    }
    if(cnt_id[2] == "15"){
        obj.filab_15["modifiedAt"] = ct;
        if(cnt_id[0] == "temp"){
            if(cinObj.con[3] != '0'){
                obj.filab_15["temperature"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_15["temperature"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "hum"){
            obj.filab_15["humidity"].value=parseFloat(cinObj.con);
        }
        else if(cnt_id[0] == "light"){
            if(cinObj.con[4] != '0'){
                obj.filab_15["illuminance"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_15["illuminance"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "pir"){
            obj.filab_15["pir"].value=parseFloat(cinObj.con);
        }
    }
    if(cnt_id[2] == "16"){
        obj.filab_16["modifiedAt"] = ct;
        if(cnt_id[0] == "temp"){
            if(cinObj.con[3] != '0'){
                obj.filab_16["temperature"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_16["temperature"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "hum"){
            obj.filab_16["humidity"].value=parseFloat(cinObj.con);
        }
        else if(cnt_id[0] == "light"){
            if(cinObj.con[4] != '0'){
                obj.filab_16["illuminance"].value=parseFloat(cinObj.con);
            }
            else{
                obj.filab_16["illuminance"].value=parseFloat(cinObj.con)+0.1;
            }
        }
        else if(cnt_id[0] == "pir"){
            obj.filab_16["pir"].value=parseFloat(cinObj.con);
        }
    }
    // console.log(obj);

}

setInterval(function(){
    filab_upload();
}, 300000);

function filab_upload(){
    var arr = []
    for(var i = 0; i< list_keys.length; i++){
        arr[0] = obj[list_keys[i]];
        console.lo
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
                    'nct': 2,
                    'exc': 0 
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
