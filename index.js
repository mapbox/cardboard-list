var stream = require('stream');

var createListParams = require('./create-list-params');

module.exports = function(cardboard, listTable) {
    
    cardboard.list = function(dataset, pageOptions, callback) {

        if (typeof pageOptions === 'function') {
            callback = pageOptions;
            pageOptions = {};
        }

        var params = createListParams(dataset, pageOptions, listTable);

        cardboard.dyno.query(params, function(err, data) {
            if (err) return callback(err);
            var ids = data.Items.map(function(item) { return item.index.replace(/^feature_id!/, ''); });
            cardboard.utils.resolveFeaturesByIds(dataset, ids, function(err, features) {
                if (err) return callback(err);
                callback(null, features);
            });
        });
    }; 

    cardboard.listStream = function(dataset, pageOptions) {
        var params = createListParams(dataset, pageOptions, listTable);
        var resolver = new stream.Transform({ objectMode: true, highWaterMark: 50 });

        resolver.items = [];

        resolver._resolve = function(callback) {
            cardboard.utils.resolveFeaturesByIds(dataset, resolver.items, function(err, collection) {
                if (err) return callback(err);
                resolver.items = [];

                collection.features.forEach(function(feature) {
                    resolver.push(feature);
                });

                callback();
            });
        };

        resolver._transform = function(item, enc, callback) {
            resolver.items.push(cardboard.utils.idFromRecord(item));
            if (resolver.items.length < 25) return callback();

            resolver._resolve(callback);
        };

        resolver._flush = function(callback) {
            if (!resolver.items.length) return callback();

            resolver._resolve(callback);
        };

        return cardboard.dyno.queryStream(params)
            .on('error', function(err) {
                resolver.emit('error', err);
            })
          .pipe(resolver); 
    };

    return cardboard;
}

module.exports.streamHandler = require('./lib/stream-handler');
