/**
 * Created by Jay on 2015/8/27.
 */
const JWT = require('jsonwebtoken');
const CODES = require("../ErrorCodes");

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

            let payload = JWT.verify(auth, this.config.secret);
            if (!payload || !String(payload.id).hasValue() ) {
                throw Error.create(CODES.SESSION_ERROR, 'auth expired or invalid');
            }
            if (callBack) return callBack(null, payload);
            resolve(payload);
        } catch (err) {
            if (callBack) return callBack(err);
            reject(err);
        }
    });
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
