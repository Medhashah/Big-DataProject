const fs = require('fs');
const JSON5 = require('json5')

// task1.json
var jsons = fs.readdirSync('.').filter(e => e.includes('task1.')).map(e => JSON5.parse(fs.readFileSync(e)));
var ret = { datasets: jsons };
for (const { columns } of ret.datasets) {
    for (const { data_types } of columns) {
        const { length } = data_types;
        if (length > 1) { }
        const int_cap = (data_types.find(({ type }) => type == 'INTEGER (LONG)') || {}).count;
        const real_cap = (data_types.find(({ type }) => type == 'REAL') || {}).count;
        const date_cap = (data_types.find(({ type }) => type == 'DATE/TIME') || {}).count;
        if (real_cap && real_cap === int_cap)
            data_types.splice(data_types.findIndex(({ type }) => type == 'REAL'));
        if (date_cap && (date_cap === int_cap || date_cap === real_cap))
            data_types.splice(data_types.findIndex(({ type }) => type == 'DATE/TIME'));
    }
}
fs.writeFileSync('task1.json', JSON.stringify(ret, null, 2));

// task1 stat
var stat = {};
for (const { columns } of ret.datasets) {
    for (const { data_types } of columns) {
        for (const { type } of data_types) {
            stat[type] = (stat[type] || 0) + 1;
        }
    }
}

// task1 #heterogeneous
var stat = 0;
for (const { columns } of ret.datasets) {
    for (const { data_types } of columns) {
        if (data_types.length > 1) stat++;
    }
}

// task1 co-occur
var stat = {};
for (const { columns } of ret.datasets) {
    for (const { data_types } of columns) {
        if (data_types.length > 1) {
            const sort = data_types.map(({ type }) => type).sort();
            stat[sort] = (stat[sort] || 0) + 1;
        }
    }
}

// task2.json
var jsons = fs.readdirSync('task2').filter(e => e.includes('.json')).map(e => ({ ...JSON5.parse(fs.readFileSync('task2/' + e)), column_name: e.slice(0, -'.json'.length) }));
var ret = { predicted_types: jsons };
fs.writeFileSync('task2.json', JSON.stringify(ret, null, 2));

// task2-manual-labels.json
var csv = fs.readFileSync('./DF_Label.csv', 'utf8').split('\n').map(e => e.split(',').map(e => e.trim()).filter(e => e));
var csv = csv.slice(1);
var ret = {
    actual_types: csv.map(([column_name, ...labels]) => ({
        column_name: column_name.slice(0, -'.txt.gz'.length),
        manual_labels: labels.map(semantic_type => ({ semantic_type })),
    }))
}
fs.writeFileSync('task2-manual-labels.json', JSON.stringify(ret, null, 2));

// 2e
var [correct_postive, predict_postive, truth_postive] = a.split('\n').map(e => JSON.parse(e))
console.log('precision')
Object.fromEntries(Object.entries(correct_postive).map(([k, v]) => [k, v / predict_postive[k]]))
console.log('recall')
Object.fromEntries(Object.entries(correct_postive).map(([k, v]) => [k, v / truth_postive[k]]))
