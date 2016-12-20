module.exports = function createListParams(dataset, pageOptions) {
    var params = {};

    pageOptions = pageOptions || {};
    if (pageOptions.start) params.ExclusiveStartKey = {
        dataset: dataset,
        key: pageOptions.start
    };
    if (pageOptions.maxFeatures) params.Limit = pageOptions.maxFeatures;

    params.ExpressionAttributeNames = { '#dataset': 'dataset' };
    params.ExpressionAttributeValues = { ':dataset': dataset };
    params.KeyConditionExpression = '#dataset = :dataset';
    return params;
}

