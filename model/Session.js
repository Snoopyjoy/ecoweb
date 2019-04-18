/**
 * Created by Jay on 2015/8/27.
 */
const JWT = require('jsonwebtoken');
const CODES = require("../ErrorCodes");
const Redis = require("../model/Redis");

function Session() {
    if (!Session.$instance) Session.$instance = this;
    this.config = {};
}

Session.prototype.init = function (params) {
    this.config = params || {};
}

Session.prototype.save = function (user, extra ) {
    callBack = arguments[arguments.length - 1];
    if (typeof callBack !== "function") callBack = null;
    extra = typeof extra === "object" ? extra : null;

    let userID = (user.id ? user.id : user.userid) || user._id;

    let loginTime = Date.now();
    let expireTime = this.config.tokenExpireTime;

    let sess = { ...extra };
    sess.id = userID;
    sess.userid = userID;
    sess.type = user.type;
    sess.loginTime = loginTime;
    return JWT.sign(sess, this.config.secret, { expiresIn: expireTime });
}

Session.prototype.check = function (auth, callBack) {
    return new Promise(async (resolve, reject) => {
        try {
            if (!this.config.secret) {
                throw Error.create(CODES.SESSION_ERROR, 'session is not configed correctly');
            }
            let options;
            if( this.config.algorithms ){
              options = { algorithms : this.config.algorithms };
            }

            let payload = JWT.verify(auth, this.config.secret, options );
            if (!payload || !String(payload.id).hasValue() ) {
                throw Error.create(CODES.SESSION_ERROR, 'auth expired or invalid');
            }
            const entry = payload.iss || "default";
            await this.checkBlock(payload.id, entry, payload.iat);
            if (callBack) return callBack(null, payload);
            resolve(payload);
        } catch (err) {
            if (callBack) return callBack(err);
            reject(err);
        }
    });
}

/**
 * @description block old user jwt
 * @param userId
 * @param maxAge
 * @param beforeTime
 * @param entry
 * @param actTime
 * @returns {Promise<any>}
 */
Session.prototype.block = function( userId, maxAge, beforeTime, entry='default', actTime='') {
    return new Promise( async (resolve, reject)=>{
        try{
            await Redis.set( `${userId}_${entry}`, `${beforeTime}_${actTime}`, maxAge );
            resolve({});
        }catch(err){
            reject(err);
        }
    } );
}

/**
 * @description check auth is blocked by ist
 * @param userId
 * @param entry
 * @param time
 * @returns {Promise<any>}
 */
Session.prototype.checkBlock = function( userId, entry, time ){
    return new Promise( async (resolve, reject)=>{
        try{
            const notBeforeDesc = await Redis.get( `${userId}_${entry}` );
            if( notBeforeDesc ){
              const beforeArr = notBeforeDesc.split('_');
              const notBefore = Number(beforeArr[0]);
              const actTime = Number(beforeArr[1]);
              const nowTimeStamp = (Date.now()/1000)>>0;
              if( actTime > nowTimeStamp) return resolve({});   //黑名单还没到激活时间
              if( Number(notBefore) > time ){
                throw Error.create(CODES.SESSION_ERROR, 'auth blocked');
              }
            }
            resolve({});
        }catch(err){
            reject(err);
        }
    } );
}

Session.getSharedInstance = function () {
    let ins = Session.$instance;
    if (!ins) {
        ins = new Session();
        Session.$instance = ins;
    }
    return ins;
}

Session.setSharedInstance = function (ins) {
    Session.$instance = ins;
}

module.exports = Session;
