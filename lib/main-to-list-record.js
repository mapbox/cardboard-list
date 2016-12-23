var utils = require('@mapbox/cardboard/lib/utils');

module.exports = function(mainRecord) {
    var datasetId = mainRecord.key.split('!')[0];
    var featureId = utils.idFromRecord(mainRecord);

    return {
        dataset: datasetId,
        key: featureId
    };
}

