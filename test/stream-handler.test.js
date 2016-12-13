var Dyno = require('dyno');
var dynamodbTest = require('dynamodb-test');
var tape = require('tape');
var utils = require('cardboard/lib/utils');

var listTableSpec = require('../lib/list-table.json');
var listTable = dynamodbTest(tape, 'cardboard-list', listTableSpec);

var mainToListRecord = require('../lib/main-to-list-record');

var CardboardList = require('..');

var listConfig = {
    listTable: listTable.tableName,
    endpoint: 'http://localhost:4567',
    region: 'test'
};

listTable.start();

var states = require('./data/states.json').features.map(function(state) {
    state.id = state.properties.name.toLowerCase().replace(/ /g, '-') + '-state';
    return utils.toDatabaseRecord(state, 'default');
});

var stateRecords = states.map(function(item) { return mainToListRecord(item, 'default'); });

var countries = require('./data/countries.json').features.map(function(feature) {
    feature.id = feature.properties.name.toLowerCase().replace(/ /g, '-') + '-country';
    return utils.toDatabaseRecord(feature, 'default');
});

var countryRecords = countries.map(function(item) { return mainToListRecord(item, 'default'); });

listTable.test('check adding on an empty db works', function(assert) {

    var cardboardList = CardboardList(listConfig);
    var records = toEvent('INSERT', states);
    cardboardList.streamHandler(records, function(err) {
        if (err) return assert.ifError(err, 'ran stream handler without error');
        cardboardList.listFeatureIds('default', function(err, ids) {
            if (err) return assert.ifError(err, 'queried db without error'); 
            assert.equal(ids.length, states.length, 'everything was inserted');
            assert.ok(ids.indexOf('new-hampshire-state') !== -1, 'found new hampshire');
            assert.end();
        });
    });
});

listTable.test('check adding on a filled db works', stateRecords, function(assert) {
    var cardboardList = CardboardList(listConfig);
    var records = toEvent('INSERT', countries);
    cardboardList.streamHandler(records, function(err) {
        if (err) return assert.ifError(err, 'ran stream handler without error');
        cardboardList.listFeatureIds('default', function(err, ids) {
            if (err) return assert.ifError(err, 'queried db without error'); 
            assert.equal(ids.length, states.length + countries.length, 'everything was inserted');
            assert.ok(ids.indexOf('swaziland-country') !== -1, 'found swaziland');
            assert.ok(ids.indexOf('new-hampshire-state') !== -1, 'found new hampshire');
            assert.end();
        });
    });
});

listTable.test('check update on a filled db works', stateRecords, function(assert) {
    var cardboardList = CardboardList(listConfig);
    var records = toEvent('MODIFY', states);
    cardboardList.streamHandler(records, function(err) {
        if (err) return assert.ifError(err, 'ran stream handler without error');
        cardboardList.listFeatureIds('default', function(err, ids) {
            if (err) return assert.ifError(err, 'queried db without error'); 
            assert.equal(ids.length, states.length, 'everything was inserted');
            assert.ok(ids.indexOf('new-hampshire-state') !== -1, 'found new hampshire');
            assert.end();
        });
    });
});

listTable.test('check removing on an filled db works', stateRecords, function(assert) {
    var cardboardList = CardboardList(listConfig);
    var records = toEvent('REMOVE', states);

    cardboardList.streamHandler(records, function(err) {
        if (err) return assert.ifError(err, 'ran stream handler without error');
        cardboardList.listFeatureIds('default', function(err, ids) {
            if (err) return assert.ifError(err, 'queried db without error'); 
            assert.equal(ids.length, 0, 'everything was remove');
            assert.end();
        });
    });
});

listTable.test('check removing doesnt removing everything', stateRecords.concat(countryRecords), function(assert) {
    var cardboardList = CardboardList(listConfig);
    var records = toEvent('REMOVE', states);
    cardboardList.streamHandler(records, function(err) {
        if (err) return assert.ifError(err, 'ran stream handler without error');
        cardboardList.listFeatureIds('default', function(err, ids) {
            if (err) return assert.ifError(err, 'queried db without error'); 
            assert.equal(ids.length, countries.length, 'everything was inserted');
            assert.ok(ids.indexOf('swaziland-country') !== -1, 'found swaziland');
            assert.end();
        });
    });
});

listTable.close();

function toEvent(action, records) {
    return {
        records: records.map(function(mainRecord) {
            var serialized = JSON.parse(Dyno.serialize(mainRecord));
            var record = { eventName: action };
            record.dynamodb = {};
            record.dynamodb.OldImage = action !== 'INSERT' ? serialized : undefined;
            record.dynamodb.NewImage = action !== 'REMOVE' ? serialized : undefined;
            return record;
        })
    };
}
