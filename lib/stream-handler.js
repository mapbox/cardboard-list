var Dyno = require('dyno');
var mainToListRecord = require('./main-to-list-record');

module.exports = function(config) {
    return function(records, callback) {
        var requestItems = records.map(function(record) {
            var change = {};
            change.before = record.dynamodb.OldImage ?
                Dyno.deserialize(JSON.stringify(record.dynamodb.OldImage)) : undefined;
            change.after = record.dynamodb.NewImage ?
                Dyno.deserialize(JSON.stringify(record.dynamodb.NewImage)) : undefined;
            change.action = record.eventName;
            return change;
        }).filter(function(change) {
            // make sure we're dealing with the feature object
            var idx = change.after ? change.after.index : change.before.index;
            return idx.split('!')[1] === 'feature';
        }).filter(function(change) {
            // filter out no change events
            if (change.action === 'MODIFY') return false;
            return true;
        }).map(function(change) {
            if (change.before === undefined) {
                return {
                    PutRequest: {
                        Item: mainToListRecord(change.after)
                    }
                };
            }
            else {
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
    }
}

