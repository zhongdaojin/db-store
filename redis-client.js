module.exports = (function() {
    var REDIS_HOST = 'localhost';
    var REDIS_PORT = 40998;
    var REDIS_PASSWORD = '';
    var REDIS_DB_INDEX = 1;
    var JST = 9;                //時差を考慮 日本時間はUTC時間+9時間
    var EXPIRE_MONTH = 3;       //キャッシュが消えるまでにかかる月
    var EXPIRE_DATE = 1;        //キャッシュが消えるまでにかかる日数(初日不算入)
    var EXPIRE_HOUR = 3 + JST;  //キャッシュが消える時刻
    
    var moment = require('moment');
    var Promise = require('bluebird');
    var Redis = Promise.promisifyAll(require("redis"));
    var client;//redis.createClient(REDIS_PORT, REDIS_HOST);
    
    var createHashKey = function(userId, closingTimestamp) {
        return userId+'@'+closingTimestamp;
    };
    
    var createHashKeyAsync = function(userId, closingTimestamp) {
        return Promise.resolve(createHashKey(userId, closingTimestamp));
    };

    var init = function() {
        client = Redis.createClient(REDIS_PORT, REDIS_HOST);
        client.auth(REDIS_PASSWORD);
        client.select(REDIS_DB_INDEX);
        client.on("error", function (err) {
            console.log("Error " + err);
        });
        console.log('REDIS INIT');
        return Promise.resolve(client);
    };
    
    var delAsync = function(hashKeys) {
        return client.delAsync(hashKeys).then(function(replies) {
            return Promise.resolve(replies);
       });
    };
    
    var hsetAsync = function(hashKey, field, value) {
        return client.hsetAsync(hashKey, field, value).then(function() {
            return client.expireatAsync(hashKey,  expirationTimestamp() );
        });
    };
    
    var hgetAsync = function(hashKey, field) {
        return client.hgetAsync(hashKey, field).then(function(replies) {
            return Promise.resolve(replies);
       });
    };
    
    var hmsetAsync = function(hashKey, fields) {
        return client.hmsetAsync(hashKey, fields).then(function() {
            return client.expireatAsync(hashKey,  expirationTimestamp() );
        });
    };
    
    var hmgetAsync = function(hashKey, fields) {
        return client.hmgetAsync(hashKey, fields).then(function(replies) {
            return Promise.resolve(replies);
       });
    };
    
    var hgetallAsync = function(hashKey) {
        return client.hgetallAsync(hashKey).then(function(replies) {
            return Promise.resolve(replies);
       });
    };
    
    var hincrbyAsync = function(hashKey, field, value) {
        return client.hincrbyAsync(hashKey, field, value).then(function(replies) {
            return Promise.resolve(replies);
       });
    };
    
    var lpushAsync = function(hashKey, contents) {
        return client.lpushAsync(hashKey, contents).then(function() {
            return client.expireatAsync(hashKey,  expirationTimestamp() );
        });
    };
    
    var rpushAsync = function(hashKey, contents) {
        return client.rpushAsync(hashKey, contents).then(function() {
            return client.expireatAsync(hashKey,  expirationTimestamp() );
        });
    };
    
      var end = function() {
        return client.end();
    };
    
    var quit = function() {
        return client.quit();
    };
    
    var expirationTimestamp = function(day, month) {
        return moment(expirationDay(day, month)).unix();
    };
    
    var expirationDay = function (day, month) {
        if(typeof day === 'undefined') day = EXPIRE_DATE;
        if(typeof month === 'undefined') month = EXPIRE_MONTH;
        var today = new Date();
        var nowDate = today.getDate();
        var nowMonth = today.getMonth()
        var d = new Date(today.getFullYear(), nowMonth, nowDate);
        d.setMonth(nowMonth + month);
        d.setDate(nowDate + day);
    
        return {
            year: d.getFullYear(),
            month: d.getMonth(),
            date: d.getDate(),
            hour: EXPIRE_HOUR,
        };
    };
    
    return {
        init: function(info) {
            return init();
        },
        createHashKey: function(userId, closingTimestamp) {
            return createHashKey(userId, closingTimestamp);
        },
        createHashKeyAsync: function(userId, closingTimestamp) {
            return createHashKeyAsync(userId, closingTimestamp);
        },
        delAsync: function(hashKeys) {
            return delAsync(hashKeys);
        },
        hsetAsync: function(hashKey, field, value) {
           return hsetAsync(hashKey, field, value);
        },
        hgetAsync: function(hashKey, field) {
           return hgetAsync(hashKey, field);
        },
        hmsetAsync: function(hashKey, fields) {
           return hmsetAsync(hashKey, fields);
        },
        hmgetAsync: function(hashKey, fields) {
           return hmgetAsync(hashKey, fields);
        },
        hgetallAsync: function(hashKey) {
           return hgetallAsync(hashKey);
        },
        hincrbyAsync: function(hashKey, field, value) {
           return hincrbyAsync(hashKey, field, value);
        },
        rpushAsync: function(hashKey, contents) {
           return rpushAsync(hashKey, contents);
        },
        quit: function() {
            return quit();
        },
        end: function() {
            return end();
        },
    };

})();