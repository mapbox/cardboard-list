var mainToListRecord = require('./main-to-list-record');
var streamHelper = require('cardboard').streamHelper;

module.exports = function(config) {
    return streamHelper(['INSERT', 'REMOVE'], function(records, callback) {
        var requestItems = records.map(function(change) {
            if (change.before === undefined) {
                return {
                    PutRequest: {
                        Item: mainToListRecord(change.after)
                    }
                };
            }
            else { // We are skipping updates above, so this is a delete
                return {
                    DeleteRequest: {
                        Key: mainToListRecord(change.before)
                    }
                }
            }
        });

        if (requestItems.length === 0) return setTimeout(callback, 0);

        var params = { RequestItems: {} };
        params.RequestItems[config.listTable] = requestItems;

        config.dyno.batchWriteAll(params).sendAll(10, function(err, res) {
            if (err) return callback(err);
            if (res.UnprocessedItems.length > 0) return callback(new Error('Not all records were written'));
            callback();
        });
            
    });
}

