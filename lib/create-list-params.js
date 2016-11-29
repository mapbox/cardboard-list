module.exports = function createListParams(dataset, pageOptions, listTable) {
    var params = { TableName: listTable };

    pageOptions = pageOptions || {};
    if (pageOptions.start) params.ExclusiveStartKey = {
        dataset: dataset,
        index: 'feature_id!' + pageOptions.start
    };
    if (pageOptions.maxFeatures) params.Limit = pageOptions.maxFeatures;

    params.ExpressionAttributeNames = { '#index': 'index', '#dataset': 'dataset' };
    params.ExpressionAttributeValues = { ':index': 'feature_id!', ':dataset': dataset };
    params.KeyConditionExpression = '#dataset = :dataset and begins_with(#index, :index)';
    return params;
}

