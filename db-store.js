module.exports = (function() {

    var mysql = require('mysql2');
    var moment = require('moment');
    var Promise = require('bluebird');
    Promise.promisifyAll(require('mysql2/lib/packet_parser'));
    var Mysql = Promise.promisifyAll(require('mysql2'));
    
    var LIST_STATUS = {
        JUDGEMENT_BEFORE: 1,
        JUDGEMENT_MIDDLE: 2,
        JUDGEMENT_AFTER : 3
    };
    
    var IS_FREEDIAL = {
        DISPLAY : 1,
        NO_DISPLAY: 2,
    };
    
    var IS_COMPLETE = {
        NOT_YET : 1,
        DONE: 2,
    };
    
    var connection = null;

    var init = function(info) {
        return new Promise(function (resolve, reject) {
            connection = Mysql.createConnection(info);
            connection.config.namedPlaceholders = true;

            connection.connect(function(err) {
                if (err) {
                    console.error('error connecting: ' + err.stack);
                    reject(err);
                }
                console.log('connected as id ' + connection.connectionId);
                resolve(connection);
            });
        });
    };
    
    var end = function() {
        connection.end();
    };
    
    var fetchListData = function(sql) {
        var calleeName = 'fetchListData';
        return new Promise(function (resolve, reject) {
            connection.prepareAsync(sql)
            .then(function(stmt) {
               return stmt.execute([ LIST_STATUS.JUDGEMENT_BEFORE ], function(err, rows, column) {
                   if (err) { return reject(calleeName+' : '+err); };
                   stmt.close();
                   return resolve(rows); 
               });
            });
        });
    };
    
    var updateJudgementResultData = function(sql, params) {
        var calleeName = 'updateJudgementResultData';
        return new Promise(function (resolve, reject) {
            connection.prepareAsync(sql)
            .then(function(stmt) {
               return stmt.execute( params, function(err, rows, column) {
                   if (err) { return reject(calleeName+' : '+err); };
                   stmt.close();
                   return resolve(rows); 
               });
            });
        });
        
    };
    
    var getListDataWithContentsQuery = function() {
        var sql = ' /* m_lists, t_list_contents */ ';
        sql += ' SELECT ';
        sql += '     A.LIST_ID ';
        sql += '    ,A.LINAGE ';
        sql += '    ,B.LIST_CONTENTS_ID ';
        sql += '    ,B.TELEPHONE_NUMBER ';
        sql += '    ,B.POSTAL_CODE ';
        sql += '    ,B.ADDRESS ';
        sql += '    ,C.USER_ID ';
        sql += '    ,C.CLOSING_TIMESTAMP ';
        sql += '    ,D.NUMBER';
        sql += '    ,D.AREA_CODE ';
        sql += '    ,D.LOCAL_OFFICE_NUMBER ';
        sql += '    ,RIGHT(B.TELEPHONE_NUMBER, 4) AS SUB_SCRIBER_NUMBER ';
        sql += '    ,E.COMPANY_ID ';
        sql += '    ,E.DEFAULT_NAME ';
        sql += '    ,E.FUNCTION_NAME ';
        sql += ' FROM m_lists A';
        sql += ' INNER JOIN t_list_contents B ';
        sql += '    ON A.LIST_ID = B.LIST_ID ';
        sql += ' INNER JOIN m_upload_files C ';
        sql += '    ON A.FILE_ID = C.FILE_ID ';
        sql += ' LEFT JOIN m_ma_list_contents D ';
        sql += '    ON SUBSTRING(B.TELEPHONE_NUMBER, 1, 6) = D.NUMBER ';
        sql += ' LEFT JOIN r_designated_companies E ';
        sql += '    ON D.DESIGNATED_COMPANY = E.COMPANY_NAME ';
        sql += ' WHERE A.LIST_STATUS_ID = ? ';
        sql += '   AND B.IS_COMPLETE = ' + IS_COMPLETE.NOT_YET;
        return Promise.resolve(sql);
    };
    
    var getListDataQuery = function() {
        var sql = ' /* m_lists */ ';
        sql += ' SELECT ';
        sql += '     A.LIST_ID ';
        sql += '    ,A.LINAGE ';
        sql += '    ,B.USER_ID ';
        sql += '    ,B.CLOSING_TIMESTAMP ';
        sql += ' FROM m_lists A';
        sql += ' INNER JOIN m_upload_files B ';
        sql += '    ON A.FILE_ID = B.FILE_ID ';
        sql += ' WHERE A.LIST_STATUS_ID = ? ';
        return Promise.resolve(sql);
    };
    
    var getUpdateListContentsQuery = function() {
        var sql = ' /* m_lists, t_list_contents */ ';
        sql += ' UPDATE ';
        sql += '    m_lists A ';
        sql += '   ,t_list_contents B ';
        sql += ' SET ';
        sql += '     A.COMPLETIONS      = A.COMPLETIONS + 1';
        sql += '    ,A.LIST_STATUS_ID   = CASE WHEN A.LINAGE = A.COMPLETIONS ';
        sql += '                               THEN ' + LIST_STATUS.JUDGEMENT_AFTER;
        sql += '                               ELSE ' + LIST_STATUS.JUDGEMENT_BEFORE;
        sql += '                          END ';
        sql += '    ,B.IS_FREEDIAL      = ? ';
        sql += '    ,B.IS_COMPLETE      = ? ';
        sql += '    ,A.UPDATE_USER      = ?';
        sql += '    ,A.UPDATE_USER_ID   = ?';
        sql += '    ,B.UPDATE_USER      = ?';
        sql += '    ,B.UPDATE_USER_ID   = ?';
        sql += ' WHERE  1 ';
        sql += '    AND A.LIST_ID          = ? ';
        sql += '    AND B.LIST_CONTENTS_ID = ? ';
        return Promise.resolve(sql);
    };
    
    return {
        IS_FREEDIAL: IS_FREEDIAL,
        IS_COMPLETE: IS_COMPLETE,
        init: function(info) {
            return init(info);
        },
        end: function() {
            return end();
            
        },
        fetchListData: function() {
            return getListDataQuery()
            .then(function(sql) {
                return fetchListData(sql);
            });
        },
        fetchListContentsData: function() {
            return getListDataWithContentsQuery()
            .then(function(sql) {
                return fetchListData(sql);
            });
        },
        updateJudgementResultData: function(params) {
            return getUpdateListContentsQuery()
            .then(function(sql) {
                return updateJudgementResultData(sql, params); 
            });
        }
    };

})();