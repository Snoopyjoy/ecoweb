/**
 * Created by Jay on 2016/5/10.
 */

const Utils = require("./../utils/Utils");
const CODES = require("./../ErrorCodes");
const Redis = require("../model/Redis");
const WebApp = require("../web/WebApp");

let Setting = global.SETTING;
const DEBUG = global.VARS.debug;

let redisSub, redisPub;
const server_notifyHandlers = {};
const client_registerHandler = {};
const defaultPingTime = 30000;      //30秒上报一次
const defaultTimeout = 60000;      //1分钟超时
const defaultReqTimeout = 15000;   //请求15秒超时
const EcoRedisKey = "EcoClients";
const OnlineServers = {};           //在线服务标识
let ecoID;                          //唯一标识
let pingTimerID;                    //心跳ID
const EocChannel = "Ecosystem";
const MessageTypeRegister= 0;        //消息类型新服务器注册
const MessageTypeNotify  = 1;        //消息类型通知
const MessageTypeApi     = 2;        //消息类型API
const MessageTypeApiAnswer     = 3;        //消息类型API成功处理
let msgID = 0;
const reqMap = {};                  //请求哈希对象
const reqs = [];                    //请求列表
const GTimer = require("../utils/GTimer");
let timer;

exports.getSetting = function() {
    return Setting.ecosystem;
}

async function updateOnlineServers(){
    const ecoSetting = exports.getSetting();
    const timeout = ecoSetting.timeout || defaultTimeout;
    const now = Date.now();
    const startTime = now - timeout;
    const ecoKey =  Redis.join( `@common->${EcoRedisKey}` );
    await Redis.do( "ZREMRANGEBYSCORE", [ ecoKey, 0, startTime ] ); //移除超时节点
    const serverIDs = await Redis.do( "ZRANGEBYSCORE", [ ecoKey, startTime, "+inf" ] );
    clearServers();
    serverIDs.forEach( serverID => {
        addServer( serverID );
    } );
}

function clearServers(){
    for (let onlineServersKey in OnlineServers) {
        OnlineServers[onlineServersKey] = null;
    }
}

function addServer( serverID ){
    const tagIndex = serverID.indexOf("_");
    const versionTagIndex = serverID.lastIndexOf("_");
    const group = serverID.substring(0,tagIndex);
    const version = serverID.substring(versionTagIndex + 1);
    const oldServers = OnlineServers[group];
    if( isEmpty(oldServers) ){
        OnlineServers[group] = {
            version: version,
            servers: [ serverID ]
        }
    }else{
        const lastVersion = oldServers.version;
        if( Utils.compareVersion( version , lastVersion ) > 0 ){
            OnlineServers[group] = {
                version: version,
                servers: [ serverID ]
            }
        }else if( oldServers.servers.indexOf( serverID ) < 0){
            oldServers.servers.push( serverID );
        }
    }
    if( !exports[group] ){
        const client = new Client(group);
        exports.__register( group, client );
    }
}

async function ping(){
    if( ecoID ){
        await updateOnlineServers();
        await Redis.do( "ZADD", [ Redis.join( `@common->${EcoRedisKey}` ), Date.now(), ecoID ] );
    }
}

async function genUUID( group ){
    const uuid = uuidv4();
    let ecoSetting = exports.getSetting();
    const version = ecoSetting.version || "";
    const tempID = `${group}_${uuid}_${version}`;
    let groupClients = OnlineServers[ group ];
    if( groupClients && groupClients.servers.indexOf( tempID ) > -1 ){
        groupClients = null;
        return await genUUID(group);
    }else{
        groupClients = null;
        return tempID;
    }
}

function messageHandler( channel, message ){
    try{
        message = JSON.parse( message );
    }catch (e) {
        return console.error( "parse ecosystem message error:", e );
    }
    const sender = message.sender;              //消息发出者的信息  || { group:"xx", ecoID:"xx", msgID:1 }
    const type = message.type;                  //处理类型 1 事件  2 api（需要回调）
    const event = message.event;                //事件类型
    const data = message.data;
    const group = sender.group;
    const senderID = sender.ecoID;
    const senderMsgID = sender.msgID;
    switch ( type ) {
        case MessageTypeRegister:               //新节点加入
            addServer( senderID );
            break;
        case MessageTypeNotify:                 //处理通知
            let handlerList = server_notifyHandlers[group + "@" + event] || [];
            let handlerList1 = server_notifyHandlers[event] || [];
            const list = handlerList.concat( handlerList1 );
            if (list && list.length > 0) {
                list.forEach(function(handler) {
                    if (handler) handler(data, group);
                });
            }
            break;
        case MessageTypeApi:                    //处理回调
            WebApp.$callAPI( event, data, function(err, result){
                let _data;
                if( err ){
                    _data = {
                        code: err.code,
                        msg: err.msg
                    }
                }else{
                    _data = {
                        code: CODES.OK,
                        data: result,
                        msg: "OK"
                    }
                }
                exports.fireTo( senderID , MessageTypeApiAnswer, senderMsgID, event, _data );
            });
            break;
        case MessageTypeApiAnswer:              //api请求成功
            const _msgID = sender.msgID;
            const task = reqMap[_msgID];
            for (let i = 0; i < reqs.length; i++) {
                if (reqs[i][0] === _msgID) {
                    reqs.splice(i, 1);
                    break;
                }
            }
            const callback = task[4];
            const timeroutID = task[5];
            delete reqMap[_msgID];
            timer.removeTimer( timeroutID );
            const code = data.code;
            const result = data.data;
            if ( code === 1 ) {
                callback && callback( null, result);
            } else {
                callback && callback( Error.create( code, data.msg ) );
            }
            __sendReq();
            break;
    }
}

exports.onServeReady = function(target, handler) {
    if (exports[target]) {
        handler && handler();
        return;
    }
    if (!client_registerHandler[target]) client_registerHandler[target] = [];
    client_registerHandler[target].push(handler);
}

exports.__register = function(target, client) {
    if (DEBUG) console.log("[Ecosystem] register *" + target + "*");
    exports[target] = client;

    if (client_registerHandler && client_registerHandler[target]) {
        client_registerHandler[target].forEach(function(handler) {
            if (handler) handler();
        });
        delete client_registerHandler[target];
    }
}

exports.callAPI = async function() {
    if (typeof arguments[0] == "string" && typeof arguments[1] == "string") {
        return await exports.__callAPI.apply(this, [ arguments[0], arguments[1], arguments[2], arguments[3] ]);
    } else {
        return await exports.__callAPI.apply(this, [ "core", arguments[0], arguments[1], arguments[2] ]);
    }
}

exports.__callAPI = function( target, method, params, callBack) {
    return new Promise(function (resolve, reject) {
        let completeFunc = function( err, result ){
            if( callBack ){
                callBack( err, result );
            }
            callBack = null;
            if(err){
                reject(err);
            }else{
                resolve(result);
            }
            reject = null;
            resolve = null;
        };
        msgID ++;
        const task = [ msgID, target, method, params, completeFunc];
        reqs.push(task);
        reqMap[msgID] = task;
        if ( reqs.length === 1) {
            const timeoutTimerID = __sendReq();
            if( timeoutTimerID ){
                task[ 5 ] = timeoutTimerID;
            }
        }
    });
}

function __sendReq(){
    if (!reqs || reqs.length <= 0) {
        timer.stopTimer();
        return 0;
    }
    if( !timer.isWorking() ){
        timer.startTimer();
    }
    const task = reqs[0];
    const rqid = task[0];
    const target = task[1];
    const method = task[2];
    const params = task[3];
    const ecoSetting = exports.getSetting();
    const timeoutVal = ecoSetting.reqTimeout || defaultReqTimeout;
    exports.fireAPI( target, method, params );
    return timer.addTimer( function( _rqid ){
        let _task = reqMap[ _rqid ];
        for (let i = 0; i < reqs.length; i++) {
            if (reqs[i][0] === _rqid) {
                reqs.splice(i, 1);
                break;
            }
        }
        const callback = _task[4];
        delete reqMap[_rqid];
        _task = null;
        callback && callback( Error.create( -1 , "connect timeout!" ) );
        __sendReq();
    }, timeoutVal , "default" , [rqid] );
}

global.__defineGetter__('Ecosystem', function() {
    return exports;
});

function Client(name) {
    const ins = this;
    this.name = name;
    this.callAPI = function( method, params, callBack ){
        return exports.__callAPI(ins.name, method, params, callBack);
    }
    this.fire = function(event, data, callBack) {
        return exports.fire(ins.name, event, data, callBack);
    }
    this.listen = function(event, handler) {
        exports.listen(ins.name, event, handler);
    }
    this.unListen = function(event, handler) {
        exports.unListen(ins.name, event, handler);
    }
}

Client.clients = {};

exports.init = function( customSetting, callBack) {
    return new Promise( async (resolve, reject)=>{
        try{
            if( !isEmpty(customSetting) ){
                Setting = customSetting;
            }
            const redisConfig = Setting.ecosystem.redis || global.SETTING.model.redis;

            let ecoSetting = exports.getSetting();
            const client = new Client( ecoSetting.name );
            exports.__register( ecoSetting.name, client );
            await updateOnlineServers();
            ecoID = await genUUID( ecoSetting.name );
            console.log( "server ecoID: ", ecoID );
            await Redis.do( "ZADD", [ Redis.join( `@common->${EcoRedisKey}` ), Date.now(), ecoID ] );
            const pingTime = ecoSetting.pingTime || defaultPingTime;
            pingTimerID = setInterval( ping, pingTime );
            timer = new GTimer( 1000 );        //1秒一次循环

            function redisReady() {
                if (!redisSub.__ready || !redisPub.__ready ) return;
                const func = callBack;
                callBack = null;
                func && func();
                exports.fireTo( null, MessageTypeRegister, -1, "", {} );
                resolve();
            }

            redisSub = Redis.createClient(redisConfig);
            redisPub = Redis.createClient(redisConfig);
            redisSub.on( "message", messageHandler );
            let subCount = 0;
            redisSub.on( "subscribe", function(channel, count){
                subCount++;
                if( subCount === 3 ){
                    redisSub.__ready = true;
                    redisReady();
                }
            } );
            redisSub.on("connect", function() {
                redisSub.subscribe( Redis.join( EocChannel ) );            //侦听针对所有对象的广播消息
                redisSub.subscribe( Redis.join( `${EocChannel}_${ecoID}` ) );                 //侦听针对自己的广播消息
                redisSub.subscribe( Redis.join( `${EocChannel}_${ecoSetting.name}` ) );       //侦听针对本组的广播消息
            });
            redisPub.on("connect", function() {
                redisPub.__ready = true;
                redisReady();
            });
        }catch(err){
            reject(err);
        }
    } );
}

exports.broadcast = function(event, data, callBack) {
    return new Promise( async (resolve, reject)=>{
        try{
            await exports.fireTo( null, MessageTypeNotify, -1, event, data );
            if (callBack)callBack();
            resolve();
        }catch(err){
            if (callBack)callBack(err);
            reject(err);
        }
    } );
}

exports.fireAPI = function( target, event, data, callBack ){
    return new Promise( async (resolve, reject)=>{
        try{
            const groupServers = OnlineServers[target] || { servers: [] };
            const servers = groupServers.servers;
            if( servers.length > 0 ){
                const targetID = servers[ (Math.random() * servers.length)>>0 ];
                await exports.fireTo( targetID, MessageTypeApi, msgID, event, data );
                if (callBack)callBack();
                resolve();
            }else{
                reject( Error.create(CODES.REDIS_ERROR, "no available client!") );
            }
        }catch(err){
            reject(err);
        }
    } );
}

exports.fire = function(target, event, data, callBack) {
    return new Promise( async (resolve, reject)=>{
        try{
            const groupServers = OnlineServers[target] || { servers: [] };
            const servers = groupServers.servers;
            if( servers.length > 0 ){
                const targetID = servers[ (Math.random() * servers.length)>>0 ];
                await exports.fireTo( targetID, MessageTypeNotify, -1, event, data );
                if (callBack)callBack();
                resolve();
            }else{
                const err = Error.create(CODES.REDIS_ERROR, "no available client!");
                if (callBack)callBack(err);
                reject(err);
            }
        }catch(err){
            if (callBack)callBack(err);
            reject(err);
        }
    } );
}

exports.fireTo = function( targetID, type, mID, event, data, callBack ){
    return new Promise( (resolve, reject)=>{
        try{
            let pubTarget;
            if( String( targetID ).hasValue() ){
                pubTarget =  Redis.join( `${EocChannel}_${targetID}` );
            }else{
                pubTarget = Redis.join( EocChannel );
            }
            const ecoSetting = exports.getSetting();
            const pubData = {
                sender: { group: ecoSetting.name, ecoID: ecoID, msgID: mID },
                data: data,
                event: event,
                type: type
            };
            redisPub.publish( pubTarget , JSON.stringify(pubData) );
            if (callBack)callBack();
            resolve();
        }catch(err){
            if (callBack)callBack(err);
            reject(err);
        }
    } );
}

exports.fireToGroup = function( target, event, data, callBack) {
    return new Promise( async (resolve, reject)=>{
        try{
            const groupServers = OnlineServers[target] || { servers: [] };
            const servers = groupServers.servers;
            if( servers.length > 0 ){
                await exports.fireTo( target, MessageTypeNotify, -1, event, data );
                if (callBack)callBack();
                resolve();
            }else{
                const err = Error.create(CODES.REDIS_ERROR, "no available client!");
                if (callBack)callBack(err);
                reject( err );
            }
        }catch(err){
            if (callBack)callBack(err);
            reject(err);
        }
    } );
}

exports.listen = function(target, event, handler) {
    const key = target + "@" + event;
    let list = server_notifyHandlers[key];
    if (!list) {
        list = [];
        server_notifyHandlers[key] = list;
    }
    if (list.indexOf(handler) >= 0) return;
    list.push(handler);
}

exports.listenAll = function(event, handler) {
    const key = event;
    let list = server_notifyHandlers[key];
    if (!list) {
        list = [];
        server_notifyHandlers[key] = list;
    }
    if (list.indexOf(handler) >= 0) return;
    list.push(handler);
}

exports.unListen = function( target, event, handler) {
    let key = event;
    if( String(target).hasValue() ){
        key = target + "@" + event;
    }
    let list = server_notifyHandlers[key];
    if (!list)  return;

    const index = list.indexOf(handler);
    if (index >= 0) list.splice(index, 1);
}

exports.getID = function(){

    return ecoID;
}