module.exports = function createListParams(dataset, pageOptions) {
    var params = {};

    pageOptions = pageOptions || {};
    if (pageOptions.start) params.ExclusiveStartKey = {
        dataset: dataset,
        key: 'feature!' +pageOptions.start
    };
    if (pageOptions.maxFeatures) params.Limit = pageOptions.maxFeatures;

    params.ExpressionAttributeNames = { '#key': 'key', '#dataset': 'dataset' };
    params.ExpressionAttributeValues = { ':key': 'feature!', ':dataset': dataset };
    params.KeyConditionExpression = '#dataset = :dataset and begins_with(#key, :key)';
    return params;
}

