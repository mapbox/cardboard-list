var dynamodbTest = require('dynamodb-test');
var tape = require('tape');
var utils = require('cardboard/lib/utils')();

var listTableSpec = require('../lib/list-table.json');
var listTable = dynamodbTest(tape, 'cardboard-list', listTableSpec);

var mainToListRecord = require('../lib/main-to-list-record');

var CardboardList = require('..');

var listConfig = {
    listTable: listTable.tableName,
    endpoint: 'http://localhost:4567',
    region: 'test'
};

var nullIslandFeature = {
    type: 'Feature',
    geometry: {
        type: 'Point',
        coordinates: [0, 0]
    },
    properties: {}
}

var nullIslandMainRecord = utils.toDatabaseRecord(nullIslandFeature, 'default');
var nullIslandListRecord = mainToListRecord(nullIslandMainRecord);

listTable.start();

listTable.test('list', [nullIslandListRecord], function(t) {
    var cardboardList = CardboardList(listConfig);

    cardboardList.listFeatureIds('default', function(err, data) {
        t.ifError(err, 'no error');
        t.equal(data.length, 1, 'got right number of features');
        t.end();
    });
});

var states = require('./data/states.json').features.map(function(state) {
    state.id = state.properties.name.toLowerCase().replace(/ /g, '-');
    return mainToListRecord(utils.toDatabaseRecord(state, 'default'));     
});

listTable.test('list stream', states, function(t) {
    var cardboardList = CardboardList(listConfig);

    var streamed = [];

    cardboardList.listFeatureIdsStream('default')
        .on('data', function(feature) {
            streamed.push(feature);
        })
        .on('error', function(err) {
            t.ifError(err, 'stream error encountered');
        })
        .on('end', function() {
            t.equal(streamed.length, states.length, 'got all the features');
            t.ok(streamed.indexOf('new-hampshire') !== -1, 'found new hampshire');
            t.end();
        });
});

listTable.close();
