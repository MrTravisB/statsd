var mongo = require('mongodb'),
    Server = mongo.Server,
    Db = mongo.Db;

var types = [
    'cmp',
    'app',
    'cmp_app',
    'ctry',
    'cmp_ctry',
    'cmp_app_ctry',
    'app_ctry',
    'ctry'
];

function isEmptyObject(obj) {
    var name;
    for (name in obj) {
        return false;
    }
    return true;
}

var BUCKET_SIZE = 60;

var flush_stats = function flush(ts, metrics) {
    var server = new Server('localhost', 27017, {auto_reconnect: true});
    var db = new Db('cb_real_time', server);
    var i, k, ct, key, bucket, inc, data, d, type, x, temp_key, new_key;
    var aggregated = {};

    for(i in metrics.counters) {
        if (!metrics.counters.hasOwnProperty(i) || metrics.counters[i] == 0) {
            continue;
        }
        ct = parseInt(metrics.counters[i], 10);
        k = i.split('.');
        if (k[0] != 'analytics') {
            continue;
        }

        key = '';
        bucket = null;
        inc = null;
        k = k.slice(1);
        type = [];
        for(d in k) {
            data = k[d].split('-');
            if (data[0] == 'b') {
                bucket = parseInt(data[1], 10);
                continue;
            }
            if (data[0] == 'f') {
                inc = data[1].trim();
                continue;
            }
            key += data[1].trim() + '_';
            type.push(data[0].trim());
        }

        key = key.substring(0, key.length-1).split('_');
        // key = key.substring(0, key.length-1) + '|' + bucket + '|' + inc + '|' + type.join('_');
        temp_key = bucket + '|' + inc;
        for(x=0; x<key.length; x++) {
            new_key = key.slice(0, key.length-x).join('_') + '|' + temp_key + '|' + type.slice(0, type.length-x).join('_');
            if (!(new_key in aggregated)) {
                aggregated[new_key] = 0;
            }
            aggregated[new_key] += ct;
        }
    }

    if (!isEmptyObject(aggregated)) {
        console.log('db time');
        db.open(function(err, db) {
            if(!err) {
                db.collection('analytics', function(err, collection) {
                    for(i in aggregated) {
                        ct = aggregated[i];
                        i = i.split('|');
                        query = {k: i[0], b: parseInt(i[1], 10), lbl: i[3]};
                        data = {$inc: {}};
                        data['$inc'][i[2]] = ct;
                        console.log(query);
                        console.log(data);
                        collection.update(query, data, {upsert: true});
                    }
                });
            }
            db.close();
        });
    }
};


exports.init = function init(startup_time, config, events) {
    events.on('flush', flush_stats);
    return true;
};