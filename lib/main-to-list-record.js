var utils = require('cardboard/lib/utils')();

module.exports = function(mainRecord) {
    var datasetId = mainRecord.index.split('!')[0];
    var featureId = utils.idFromRecord(mainRecord);

    return {
        dataset: datasetId,
        index: 'feature!'+featureId
    };
}

