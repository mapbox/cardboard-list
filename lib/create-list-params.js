module.exports = function createListParams(dataset, pageOptions) {
    var params = {};

    pageOptions = pageOptions || {};
    if (pageOptions.start) params.ExclusiveStartKey = {
        dataset: dataset,
        index: 'feature!' +pageOptions.start
    };
    if (pageOptions.maxFeatures) params.Limit = pageOptions.maxFeatures;

    params.ExpressionAttributeNames = { '#index': 'index', '#dataset': 'dataset' };
    params.ExpressionAttributeValues = { ':index': 'feature!', ':dataset': dataset };
    params.KeyConditionExpression = '#dataset = :dataset and begins_with(#index, :index)';
    return params;
}

