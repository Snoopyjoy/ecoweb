/**
 * 通过setInterval实现timer
 * Created by hxl on 2017/9/6.
 */

function GTimer( updateDelay ){
    this.times = 0;
    this.timers = {
        default:{}
    };
    this.intervalTimer = 0;
    this.pauseTar={
        default:0
    };
    this.updateDelay = updateDelay || 1000;
    this.stepGap = updateDelay;
}

var proto = GTimer.prototype;
proto.startTimer = function(){
    var ins = this;
    this.intervalTimer = setInterval(function() {
        ins.times += ins.stepGap;
        ins.execute(true);
    }, this.updateDelay);
};

proto.stopTimer = function(){
    clearInterval( this.intervalTimer );
}

proto.execute = function( timer ){
    for( var group in this.timers ){
        if( this.pauseTar[group] == 1 ){
            continue;
        }
        var groupTimers = this.timers[group];
        for( var timerID in groupTimers ){
            var timerDef = groupTimers[timerID];
            var callback = timerDef.callback;
            var passed = timerDef.passed;
            var delay = timerDef.delay;
            if(  passed >= delay ){
                callback && callback.apply(null, timerDef.args  );
                groupTimers[timerID] = null;
                delete groupTimers[timerID];
            }
            if( timer ){
                timerDef.passed += this.stepGap;
            }
        }
    }
};

proto.pause = function( group ){
    if( group ){
        this.pauseTar[group] = 1;
    }
};

proto.continue = function( group ){
    this.pauseTar[group] = 0;   
};

/*增加倒计时 单位（秒）最小延时1秒

*/
proto.addTimer = function( callback, delay , group , agrs ){
    group = group || "default";
    if( !this.timers[group] ){
        this.timers[group] = {};
    }
    var timerID = this.getTimerID(group);
    this.timers[group][timerID] = {
        id: timerID,
        callback: callback,
        delay: delay,
        passed: 0,
        args : agrs
    };
    return timerID;
};

/*移除定时*/
proto.removeTimer = function( timerID ){
    if( String( timerID ).hasValue() ){
        var group = timerID.split("#")[0];
        if (this.timers[group]) {
            delete this.timers[group][timerID];
        }
    }
};

proto.getTimerByID = function( timerID ){
    if( !String(timerID).hasValue() ){
        return null;
    }
    var group = timerID.split("#")[0];
    if (!this.timers[group]) return null;
    return this.timers[group][timerID];
};

proto.changeTimerDelay = function( timerID, delta ){
    let timerDef = this.getTimerByID( timerID );
    if(timerDef){
        timerDef.delay = timerDef.delay + delta;
    }
};

proto.getTimerID = function(group){
    var id = group + "#" + this.times;
    var tag = 0;
    if (!this.timers[group]) return 0;
    while( this.timers[group][ id ] ){
        tag++;
        id = group + "#" + this.times + "#" + tag;
    }
    return id;
};

proto.destroy = function(){
    clearInterval( this.intervalTimer );
    this.times = 0;
    this.timers = null;
    this.intervalTimer = 0;
};

module.exports = GTimer;

