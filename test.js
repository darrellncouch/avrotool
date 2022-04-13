const fs = require('fs');
const util = require('util');
// const {
//     exec
// } = require("child_process");

const exec = util.promisify(require('child_process').exec)


async function getAvrosToJson(offer, hoursArr, minArr, name = '58' ) {
    let counter = 1;
    let out = [];
    for (let i = 0; i < hoursArr.length; i++) {
        for (let j = 0; j < minArr.length; j++) {
            await exec(`cd ./input && java -jar avro-tools-1.11.0.jar tojson ../AVROs/${offer}/${hoursArr[i]}/${minArr[j]}/${name}.avro > ${counter}.json`);
            out.push({counter: counter, hour: hoursArr[i], min: minArr[j]});
            counter++;

        }
    }

    return out;
}


function formatJSON(count) {
    let head = 'PartitionKey,RowKey,CreatedOn,Platform,RewardId,Carrier,DeviceCarrier\r\n';

    fs.writeFileSync("./output/claim.csv", head);
    fs.writeFileSync("./output/gift.csv", head);
    fs.writeFileSync("./output/redeem.csv", head);

    for (let i = 0; i < count.length; i++) {
        const file = fs.readFileSync(`./input/${count[i].counter}.json`, 'utf-8');
        const commas = file.replace(new RegExp('}"}}', 'g'), '}"}},');
        const removeLast = commas.slice(0, commas.length - 3);
        const jsons = `[${removeLast}]`;
        const parsed = JSON.parse(jsons);
        const releventInfoArr = parsed.map(x => JSON.parse(x.Body.bytes));

        const identifyer = `20220412_${count[i].hour}_${count[i].min[0] === '0' ? count[i].min[1] : count[i].min}`;

        releventInfoArr.map(item => {
            const valueStr = `"${identifyer}","${item.EventId}","${item.CreatedOn}","${item.AppInformation.Platform}","${item.RewardId}","${item.AppInformation.Carrier}","${item.AppInformation.DeviceCarrier}"\r\n`;

            if (item.Type === "Claim") {
                let file = fs.readFileSync("./output/claim.csv", 'utf-8');
                file += valueStr;
                fs.writeFileSync("./output/claim.csv", file);
            }

            if (item.Type === "Gift") {
                let file = fs.readFileSync("./output/gift.csv", 'utf-8');
                file += valueStr;
                fs.writeFileSync("./output/gift.csv", file);
            }

            if (item.Type === "Redeem") {
                let file = fs.readFileSync("./output/redeem.csv", 'utf-8');
                file += valueStr;
                fs.writeFileSync("./output/redeem.csv", file);
            }
        })


    }
}

async function run2(offer, name, minset) {
    const hoursArr = ['14'];
    let counter = 1;

    const count = await getAvrosToJson(offer, hoursArr, minset, name);

    formatJSON(count)
}

const set1 = ['01', '06', '11', '16', '21', '26', '31', '36', '41', '46', '51', '56'];
const set2 = ['02','07','12','17','22','27','32','37','42','47','52','57'];
run2('0', '58', set1);