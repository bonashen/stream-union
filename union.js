/**
 * Created by bona on 2015/6/6.
 */

var through = require("through");

function union(right, distinct) {
    var getDistinctValue = (typeof distinct == "function") ? distinct : function (item) {
        return item[distinct];
    }, map = {}, result = [], endCount = 0;

    function addMap(doc) {
        if (doc) {
            map[getDistinctValue(doc)] = map[getDistinctValue(doc)] || doc;
        }
    }

    var lefts = getStream(), rights = getStream();

    function getStream() {
        return through(function (buf) {
            addMap(buf);
            return true;
        }, function () {
            endCount++;
            return true;
        });
    }
    var hasProp ={}.hasOwnProperty;

    function processout() {
        for(var key in map){
            if(hasProp(key))
                result.push(map[key])
        }

        function next() {
            if (result.length) {
                lefts.emit("data", result.pop());
                process.nextTick(next);
            } else {
                lefts.readable = false;
                lefts.emit("end");
            }
        }

        process.nextTick(next);
    }

    function waitEnd() {
        if (endCount == 2) {
            processout();
            return;
        }
        process.nextTick(waitEnd);
    }

    right.pipe(rights);
    process.nextTick(waitEnd);
    return lefts;
}

module.exports = union;
