var Dyno = require('dyno');
var stream = require('stream');

var createListParams = require('./lib/create-list-params');

module.exports = function(config) {

    if (!config.dyno && (typeof config.listTable !== 'string' || config.listTable.length === 0)) throw new Error('"listTable" must be a string');
    if (!config.dyno && !config.region) throw new Error('No region set');
    if (!config.dyno) config.dyno = Dyno({table: config.listTable, region: config.region, endpoint: config.endpoint});
 
    var cardboardList = {};

    cardboardList.listFeatureIds = function(dataset, pageOptions, callback) {

        if (typeof pageOptions === 'function') {
            callback = pageOptions;
            pageOptions = {};
        }

        var params = createListParams(dataset, pageOptions);

        config.dyno.query(params, function(err, data) {
            if (err) return callback(err);
            var ids = data.Items.map(function(item) { return item.key; });
            callback(null, ids);
        });
    }; 

    cardboardList.listFeatureIdsStream = function(dataset, pageOptions) {
        var params = createListParams(dataset, pageOptions);
        var resolver = new stream.Transform({ objectMode: true, highWaterMark: 50 });

        resolver._transform = function(item, enc, callback) {
            resolver.push(item.key);
            callback();
        };

        return config.dyno.queryStream(params)
            .on('error', function(err) {
                resolver.emit('error', err);
            })
          .pipe(resolver); 
    };

    cardboardList.streamHandler = require('./lib/stream-handler')(config);

    return cardboardList;
}

