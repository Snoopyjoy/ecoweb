/**
 * Created by Jay on 2015/8/24.
 */

const REDIS = require("redis");
const uuidv4 = require("uuid/v4");
const EventEmitter = require("events").EventEmitter;

const Dispatcher = new EventEmitter();
let client;
let setting;
const DEBUG = global.VARS ? global.VARS.debug : false;



exports.isConnected = function() {
    return client && client.__working == true;
}

function setExpire(key, val) {
    if (!val || val == - 1) {
        //no expired
    } else {
        client.expire(exports.join(key), val);
    }
}

const EXPIRED_MAP = {};

const CACHE_PREFIX = "CACHE_";

let SEP = ".";

const lockScript = 'return redis.call("set", KEYS[1], ARGV[1], "NX", "PX", ARGV[2])';
const unlockScript = 'if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end';
const extendScript = 'if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end';
const LOCK_OPTIONS = {
	driftFactor: 0.01,      //锁过期漂移因子
	retryCount:  10,        //重试次数
	retryDelay:  200,       //重试延迟
    retryJitter: 100,       //重试延迟随机因素
    ttl: 12000              //默认12秒过期
}
const LOCK_MAP = {
    'total': 0
};
let totalLockWaitNum = 10000; //默认锁处理最大等待数 1kb
let singleLockWaitNum = 1000; //默认锁处理最大等待数 1kb

exports.addEventListener = function(type, handler) {
    Dispatcher.addListener(type, handler);
}

exports.removeEventListener = function(type, handler) {
    Dispatcher.removeListener(type, handler);
}

exports.removeAllEventListener = function() {
    Dispatcher.removeAllListeners.apply(Dispatcher, arguments);
}

exports.setExpireTime = function(key, val) {
    if (key instanceof Array) key = key.join(SEP);
    setExpire(key, val);
}

exports.registerExpiredTime = function(key, expired) {
    if (key instanceof Array) key = key.join(SEP);
    EXPIRED_MAP[key] = Number(expired);
}

exports.getCacheKey = function( key ){
  var tempKey = key;
  var redisKey = key;
  if (key instanceof Array) tempKey = key.join(SEP);
  if (tempKey.substr(0, 1) == "@") {
    redisKey = exports.join(tempKey, CACHE_PREFIX);
  } else {
    redisKey = exports.join(CACHE_PREFIX + tempKey);
  }
  return redisKey;
}

exports.save = function(key, val) {
    var callBack = typeof arguments[2] === "function" ? arguments[2] : arguments[3];
    if (typeof callBack !== "function") callBack = null;

    var expired = typeof arguments[2] === "number" ? arguments[2] : arguments[3];
    if (typeof expired !== "number") expired = null;

    return new Promise(function (resolve, reject) {
        var firstKey = key;
        var originalKey = key;
        var tempKey = key;
        if (key instanceof Array) {
            tempKey = key.join(SEP);
            firstKey = key[0];
        }
        if (!expired) expired = EXPIRED_MAP[firstKey];
        var originalVal = val;
        if (typeof val === "object") {
            val = JSON.stringify(val);
        }

        const redisKey = exports.getCacheKey(key);

        client.set(redisKey, val, function (redisErr, redisRes) {
            if (!expired || expired === - 1) {
                //no expired
            } else {
                client.expire(redisKey, expired);
            }

            if (redisRes) {
                //console.log('2 -> cache [' + key + '] saved. expired ==> ' + expired);
                Dispatcher.emit("save", tempKey, originalKey, originalVal);
            } else {
                Dispatcher.emit("error", tempKey, originalKey, redisErr);
            }
            if (callBack) return callBack(redisErr, redisRes);
            if (redisErr) {
                reject(redisErr);
            } else {
                resolve(originalVal);
            }
        });
    });
}

exports.read = function(key, callBack) {
    return new Promise(function (resolve, reject) {
        const redisKey = exports.getCacheKey(key);
        //console.log('read ---> ' + tempKey);
        client.get(redisKey, function(err, res) {
            if (res && typeof res == "string") {
                try {
                    res = JSON.parse(res);
                } catch (exp) { }
            }
            if (callBack) return callBack(err, res);
            if (err) {
                reject(err);
            } else {
                resolve(res);
            }
        });
    });
}

exports.remove = function(key, callBack) {
    return new Promise(function (resolve, reject) {
        const redisKey = exports.getCacheKey(key);
        client.del(redisKey, function(err) {
            if (callBack) return callBack(err);
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
}

exports.set = function(key, val) {
    var callBack = arguments[arguments.length - 1];
    if (typeof callBack != "function") callBack = null;
    var expired = typeof arguments[2] == "number" ? arguments[2] : null;
    if (typeof expired != "number") expired = null;

    return new Promise(function (resolve, reject) {
        client.set(exports.join(key), val, function (err, res) {
            setExpire(key, expired);
            if (callBack) return callBack(err);
            if (err) {
                reject(err);
            } else {
                resolve(res);
            }
        });
    });
}

exports.setHash = function(key, field, val) {
    var callBack = arguments[arguments.length - 1];
    if (typeof callBack != "function") callBack = null;
    var expired = typeof arguments[3] == "number" ? arguments[3] : null;
    if (typeof expired != "number") expired = null;

    return new Promise(function (resolve, reject) {
        client.hset(exports.join(key), field, val, function (err, res) {
            setExpire(key, expired);
            if (callBack) return callBack(err, res);
            if (err) {
                reject(err);
            } else {
                resolve(res);
            }
        });
    });
}

exports.setHashMulti = function(key, fieldAndVals) {
    var callBack = arguments[arguments.length - 1];
    if (typeof callBack != "function") callBack = null;
    var expired = typeof arguments[2] == "number" ? arguments[2] : null;
    if (typeof expired != "number") expired = null;

    return new Promise(function (resolve, reject) {
        client.hmset(exports.join(key), fieldAndVals, function (err, res) {
            setExpire(key, expired);
            if (callBack) return callBack(err, res);
            if (err) {
                reject(err);
            } else {
                resolve(res);
            }
        });
    });
}

exports.pushIntoList = function(key, value, callBack) {
    return new Promise(function (resolve, reject) {
        var args = [ exports.join(key) ];
        args = args.concat(value);
        args.push(function(err, res) {
            if (callBack) return callBack(err, res);
            if (err) {
                reject(err);
            } else {
                resolve(res);
            }
        })
        client.lpush.apply(client, args);
    });
}

exports.searchKeys = function( keyword, callBack ){
    return new Promise( async function( resolve, reject ){
        let keyArr = [];
        let cursor = 0;          //游标
        try {
            do{
                const result = await exports.do("SCAN" , [ cursor, "MATCH", keyword ] );
                cursor = result[0];
                const newKeys = result[1];
                keyArr = keyArr.concat( newKeys );
            }while ( Number(cursor) > 0  )
            callBack&&callBack( null, keyArr );
            resolve( keyArr );
        }catch (e) {
            callBack&&callBack(e);
            reject(e);
        }
    } );
}

exports.getFromList = function(key, fromIndex, toIndex, callBack) {
    return new Promise(function (resolve, reject) {
        client.lrange(exports.join(key), fromIndex, toIndex, function(err, res) {
            if (callBack) return callBack(err, res);
            if (err) {
                reject(err);
            } else {
                resolve(res);
            }
        });
    });
}

exports.getWholeList = function(key, callBack) {
    return exports.getFromList(key, 0, -1, callBack);
}

exports.setToList = function(key, index, value, callBack) {
    return new Promise(function (resolve, reject) {
        client.lset(exports.join(key), index, value, function(err, res) {
            if (callBack) return callBack(err, res);
            if (err) {
                reject(err);
            } else {
                resolve(res);
            }
        });
    });
}

exports.get = function(key, callBack) {
    return new Promise(function (resolve, reject) {
        client.get(exports.join(key), function(err, res) {
            if (callBack) return callBack(err, res);
            if (err) {
                reject(err);
            } else {
                resolve(res);
            }
        });
    });
}

exports.getHash = function(key, field, callBack) {
    return new Promise(function (resolve, reject) {
        client.hget(exports.join(key), field, function (err, res) {
            if (callBack) return callBack(err, res);
            if (err) {
                reject(err);
            } else {
                resolve(res);
            }
        });
    });
}

exports.getHashMulti = function(key, field, callBack) {
    return new Promise(function (resolve, reject) {
        client.hmget(exports.join(key), field, function (err, res) {
            if (callBack) return callBack(err, res);
            if (err) {
                reject(err);
            } else {
                resolve(res);
            }
        });
    });
}

exports.getHashAll = function(key, callBack) {
    return new Promise(function (resolve, reject) {
        client.hgetall(exports.join(key), function (err, res) {
            if (callBack) return callBack(err, res);
            if (err) {
                reject(err);
            } else {
                resolve(res);
            }
        });
    });
}

exports.getHashKeys = function(key, callBack) {
    return new Promise(function (resolve, reject) {
        client.hkeys(exports.join(key), function (err, res) {
            if (callBack) return callBack(err, res);
            if (err) {
                reject(err);
            } else {
                resolve(res);
            }
        });
    });
}

exports.delHashField = function(key, fields, callBack) {
    return new Promise(function (resolve, reject) {
        client.hdel.apply(client, [ exports.join(key) ].concat(fields).concat(function(err, res) {
            if (callBack) return callBack(err, res);
            if (err) {
                reject(err);
            } else {
                resolve(res);
            }
        }));
    });
}

exports.findKeysAndDel = function(keyword, callBack) {
    return new Promise(function (resolve, reject) {
        exports.searchKeys(keyword, function(err, keys) {
            if (err) {
                if (callBack) return callBack(err);
                return reject(err);
            } else {
                keys = keys || [];
                if (keys.length <= 0) {
                    if (callBack) return callBack(null, 0);
                    return resolve(0);
                }

                var tasks = [];
                keys.forEach(function(key) {
                    tasks.push([ "del", key ]);
                });
                exports.multi(tasks, function(err) {
                    if (callBack) return callBack(err, tasks.length);
                    if (err) {
                        reject(err);
                    } else {
                        resolve(tasks.length);
                    }
                });
            }
        });
    });
}

exports.del = function(key, callBack) {
    return new Promise(function (resolve, reject) {
        client.del(exports.join(key), function(err, res) {
            if (callBack) return callBack(err, res);
            if (err) {
                reject(err);
            } else {
                resolve(res);
            }
        });
    });
}

exports.multi = function(tasks, callBack) {
    return new Promise(function (resolve, reject) {
        client.multi(tasks).exec(function (err, result) {
            if (callBack) return callBack(err);
            if (err) {
                reject(err);
            } else {
                resolve( result );
            }
        });
    });
}

exports.multiTask = function() {
    return client.multi();
}

exports.do = function (cmd, args, callBack) {
    return new Promise(function (resolve, reject) {
        var done = function(err, res) {
            if (callBack) return callBack(err, res);
            if (err) {
                reject(err);
            } else {
                resolve(res);
            }
        }
        client[cmd].apply(client, args.concat([ done ]));
    });
}

exports.subscribe = function (channel, callBack) {
    return new Promise(function (resolve, reject) {
        client.subscribe(channel, function(err) {
            if (callBack) return callBack(err);
            err ? reject(err) : resolve();
        });
    });
}

exports.publish = function (channel, message, callBack) {
    return new Promise(function (resolve, reject) {
        client.publish(channel, message, function(err) {
            if (callBack) return callBack(err);
            err ? reject(err) : resolve();
        });
    });
}

exports.on = function (event, handler) {
    client.on(event, handler);
}

exports.join = function(key, preKey) {
    var redisKey = KEY_CACHE[key];
    if (redisKey) return redisKey;

    var group = "*";
    if (key.charAt(0) == "@" && GROUP_REG.test(key)) {
        var g = key.match(GROUP_REG);
        if (g && g.length > 0) {
            redisKey = key;
            group = g[0].substr(1, g[0].length - 3);
            key = key.substring(group.length + 3);
        }
        else return null;
    }

    var prefix = groups[group];
    KEY_CACHE[redisKey] = prefix + (preKey || "") + key;
    return KEY_CACHE[redisKey];
}

/**
 * @description 节流函数
 * @param redisKey
 * @param duration 时间范围
 * @param times 触发次数
 */
exports.rateLimit = function( redisKey, duration, times , callback ){
    return new Promise(async (resolve, reject)=>{
        try {
            const fullThrottleKey = exports.join( redisKey + "_redisLock");
            const curVal = await exports.do("INCR" , [ fullThrottleKey ]);
            if( Number(curVal) === 1 ){
                await exports.do("PEXPIRE" , [ fullThrottleKey, duration ]);
            }
            if( curVal > times ){
                throw new Error("req limited!");
            }
            callback && callback();
            resolve();
        }catch (e) {
            callback && callback(e);
            reject(e);
        }
    });
}


/**
 * @description 获取一个锁操作
 * @param {string} key 资源id
 * @param {object} options 配置
 * @param {number} options.driftFactor 锁过期漂移因子
 * @param {number} options.retryCount 重试次数 -1 无限重试
 * @param {number} options.retryDelay 重试延迟 毫秒
 * @param {number} options.retryJitter 重试延迟随机因素 毫秒
 * @param {number} options.ttl 锁有效期 毫秒
 */
exports.getLocker = function( key, options ){
    const uuid = uuidv4();
    const lockKey = exports.join(key);
    return new Locker( lockKey, uuid, options );
}

exports.setLockOption = function( options ){
    if( options.totalLockWaitNum ){
        totalLockWaitNum = options.totalLockWaitNum;
    }else{
        singleLockWaitNum = options.singleLockWaitNum;
    }
}

exports.eval = function( script, ...args ){
    return new Promise( (resolve, reject)=>{
        try{
            client.eval( script, ...args, function( err, response ){
                if( err ){
                    return reject( err );
                }else{
                    return resolve( response );
                }
            });
        }catch(err){
            reject(err);
        }
    } );
}

exports.checkLock = function( locker ) {
    return new Promise( async function (resolve, reject) {
        try{
            let lockResult;
            locker.expiration = Date.now() + locker.ttl;
            if( !locker.locked ){    //
                lockResult = await exports.eval( lockScript, 1, locker.key, locker.uuid, locker.orgTTL );
            }else{                  //已经上锁,延时
                locker.attempts = 0;    //重置尝试次数
                lockResult = await exports.eval( extendScript, 1, locker.key, locker.uuid, locker.orgTTL );
            }
            if( lockResult && locker.expiration > Date.now() ){   //取锁成功
                locker.locked = true;
                if( locker.attempts > 0 ){ //重试成功
                    //减少统计次数
                    LOCK_MAP.total--;
                    LOCK_MAP[locker.key]--;
                    if( LOCK_MAP[locker.key] <= 0 ) delete LOCK_MAP[locker.key];
                }
                return resolve();
            }else{              //
                const totalWaitNum = LOCK_MAP.total;       //总等待数
                const keyWaitNum = LOCK_MAP[locker.key];    //单个资源等待数
                if( locker.attempts === 0 ){      //第一次尝试
                    if( totalWaitNum >= totalLockWaitNum ){
                        return reject( new Error("total lock limited") );
                    }
                    if( keyWaitNum && keyWaitNum > singleLockWaitNum ){
                        return reject( new Error("resource limited") );
                    }
                    //增加统计次数
                    LOCK_MAP.total++;
                    if( LOCK_MAP[locker.key] ){
                        LOCK_MAP[locker.key]++;
                    }else{
                        LOCK_MAP[locker.key] = 1;
                    }
                }else if( locker.retryCount !== -1 && locker.attempts >= locker.retryCount ){
                      //减少统计次数
                      LOCK_MAP.total--;
                      LOCK_MAP[locker.key]--;
                      if( LOCK_MAP[locker.key] <= 0 ) delete LOCK_MAP[locker.key];
                    return reject( new Error("max retry") );
                }
                locker.attempts++;  //重试次数加一
                setTimeout( function( _locker ){
                    exports.checkLock( _locker ).then( resolve ).catch( reject );
                }, locker.retryDelay, locker );
            }
        }catch(err){
            reject(err);
        }
    });
}

exports.getLockStat = function(){
    return LOCK_MAP;
}

exports.releaseLock = function( locker ) {
    return new Promise(async function (resolve, reject) {
        try{
            let response = await exports.eval( unlockScript, 1, locker.key, locker.uuid );
            if(typeof response === 'string')
				response = parseInt(response);
			if(response === 0 || response === 1){
                return resolve();
            }else{
                reject( new Error(`relase lock err *${response}`) );
            }

        }catch (err) {
            if (err) console.error(`release lock *${lockKey}* error ---> ${err}`);
            reject(err);
        }
    });
}

var groups = {};

var KEY_CACHE = {};
var GROUP_REG = /@[a-zA-Z0-9]+->/;

exports.createClient = function(config, connectCallback) {
    config = config || setting;
    var ins = REDIS.createClient(config.port, config.host, { auth_pass: config.pass ,
        retry_strategy:function(options){
            /*if (options.error && options.error.code === 'ECONNREFUSED') {
                // End reconnecting on a specific error and flush all commands with
                // a individual error
                return new Error('The server refused the connection');
            }
            if (options.total_retry_time > 1000 * 60 * 60) {
                // End reconnecting after a specific timeout and flush all commands
                // with a individual error
                return new Error('Retry time exhausted');
            }
            if (options.attempt > 10) {
                // End reconnecting with built in error
                return undefined;
            }*/
            return Math.min(options.attempt * 100, 15000);
        }
    });
    if (connectCallback) {
        ins.on("connect", function() {
            ins.removeListener("connect", arguments.callee);
            connectCallback(ins);
        });
    }
    return ins;
}

exports.start = function(option, callBack) {

    setting = option || { };
    var host = setting.host || "localhost";
    var port = setting.port || 6379;
    var pass = setting.pass || "";
    var prefixName = setting.prefix || "weroll_";

    if (setting.cache && setting.cache.group_sep) SEP = setting.cache.group_sep;

    if (typeof prefixName == "object") {
        for (var key in prefixName) {
            groups[key] = prefixName[key];
        }
    } else {
        groups["*"] = prefixName;
    }

    client = REDIS.createClient(port, host, { auth_pass: pass });
    client.__working = false;
    client.__startCallBack = callBack;

    client.on("error", function(err) {
        client.__working = false;
        console.error(err);
    });
    client.on("connect", function() {
        console.log("Redis Server<" + host + ":" + port + "> is connected.");
        client.__working = true;
        if (client.__startCallBack) {
            client.__startCallBack();
            client.__startCallBack = null;
        }
    });
}



class Locker{
    /**
     *
     * @param {string} key 资源id
     * @param {string} uuid 锁id
     * @param {object} options 配置
     * @param {number} options.driftFactor 锁过期漂移因子
     * @param {number} options.retryCount 重试次数 -1 无限重试
     * @param {number} options.retryDelay 重试延迟 毫秒
     * @param {number} options.retryJitter 重试延迟随机因素 毫秒
     * @param {number} options.ttl 锁有效期 毫秒
     */
    constructor( key, uuid, options = {} ){
        this.key = key;         //资源id
        this.uuid = uuid;       //锁id
        const defaultOption = Object.assign({},LOCK_OPTIONS);
        this.options = Object.assign( defaultOption, options ); //配置信息
        this.attempts = 0;      //重试次数
	    const drift = Math.round(this.options.driftFactor * this.options.ttl) + 2;
        this.ttl = this.options.ttl - drift;
        this.expiration = 0;    //过期时间戳
        this.locked = false;
    }

    get orgTTL(){
        return this.options.ttl;
    }

    get retryCount(){
        return this.options.retryCount;
    }

    /**
     * @description 解锁
     */
    async unlock(){
        await exports.releaseLock(this);
        this.locked = false;
    }

    /**
     * @description 获取锁
     * @param {number|null} ttl 锁有效期 毫秒
     */
    async lock( ttl ){
        if(ttl){
            const drift = Math.round( this.options.driftFactor * ttl ) + 2;
            this.ttl = ttl - drift;
        }
        await exports.checkLock(this);
        this.locked = true;
    }

    get retryDelay(){
        return Math.max(0, this.options.retryDelay + Math.floor((Math.random() * 2 - 1) * this.options.retryJitter));
    }



}
